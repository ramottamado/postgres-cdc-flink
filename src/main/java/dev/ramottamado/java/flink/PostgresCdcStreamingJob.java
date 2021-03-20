package dev.ramottamado.java.flink;

import static dev.ramottamado.java.flink.config.ParameterConfig.KAFKA_SOURCE_TOPIC_1;
import static dev.ramottamado.java.flink.config.ParameterConfig.KAFKA_SOURCE_TOPIC_2;

import java.util.Properties;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import dev.ramottamado.java.flink.functions.DestinationAccountKeySelector;
import dev.ramottamado.java.flink.functions.EnrichEnrichedTransactionsWithCustomersJoinFunction;
import dev.ramottamado.java.flink.functions.EnrichTransactionsWithCustomersJoinFunction;
import dev.ramottamado.java.flink.functions.EnrichedTransactionsToStringMapFunction;
import dev.ramottamado.java.flink.schema.Customers;
import dev.ramottamado.java.flink.schema.EnrichedTransactions;
import dev.ramottamado.java.flink.schema.Transactions;
import dev.ramottamado.java.flink.util.ParameterUtils;
import dev.ramottamado.java.flink.util.kafka.KafkaProperties;
import dev.ramottamado.java.flink.util.serialization.DebeziumJSONEnvelopeDeserializationSchema;
import dev.ramottamado.java.flink.util.serialization.EnrichedTransactionsJSONSerializationSchema;

/*
 * Stream PostgreSQL CDC from Debezium into Flink.
 */
public class PostgresCdcStreamingJob {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.setString("state.backend", "filesystem");
		conf.setString("state.savepoints.dir", "file:///tmp/savepoints");
		conf.setString("state.checkpoints.dir", "file:///tmp/checkpoints");

		ParameterTool params = ParameterUtils.parseArgs(args);
		Properties properties = KafkaProperties.getProperties(params);

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironmentWithWebUI(conf);

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
		env.enableCheckpointing(10000L);

		CheckpointConfig config = env.getCheckpointConfig();
		config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		DebeziumJSONEnvelopeDeserializationSchema<Transactions> tDeserializationSchema =
				new DebeziumJSONEnvelopeDeserializationSchema<>(Transactions.class);

		DebeziumJSONEnvelopeDeserializationSchema<Customers> cDeserializationSchema =
				new DebeziumJSONEnvelopeDeserializationSchema<>(Customers.class);

		EnrichedTransactionsJSONSerializationSchema etxSerializationSchema =
				new EnrichedTransactionsJSONSerializationSchema("enriched_transactions");

		FlinkKafkaConsumer<Transactions> tKafkaConsumer =
				new FlinkKafkaConsumer<>(params.getRequired(KAFKA_SOURCE_TOPIC_1), tDeserializationSchema, properties);

		FlinkKafkaConsumer<Customers> cKafkaConsumer =
				new FlinkKafkaConsumer<>(params.getRequired(KAFKA_SOURCE_TOPIC_2), cDeserializationSchema, properties);

		tKafkaConsumer.setStartFromEarliest();
		cKafkaConsumer.setStartFromEarliest();

		FlinkKafkaProducer<EnrichedTransactions> etxKafkaProducer =
				new FlinkKafkaProducer<>(
						"enriched_transactions", etxSerializationSchema, properties,
						FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
				);

		DestinationAccountKeySelector destinationAccountKeySelector =
				new DestinationAccountKeySelector();

		EnrichTransactionsWithCustomersJoinFunction join1 =
				new EnrichTransactionsWithCustomersJoinFunction();

		EnrichEnrichedTransactionsWithCustomersJoinFunction join2 =
				new EnrichEnrichedTransactionsWithCustomersJoinFunction();

		EnrichedTransactionsToStringMapFunction enrichedTransactionsParser =
				new EnrichedTransactionsToStringMapFunction();

		KeyedStream<Customers, String> customersCdcStream = env.addSource(cKafkaConsumer)
				.keyBy(Customers::getAcctNumber);

		KeyedStream<Transactions, String> transactionsCdcStream = env.addSource(tKafkaConsumer)
				.uid("transactions_cdc_stream")
				.keyBy(Transactions::getSrcAccount);

		SingleOutputStreamOperator<EnrichedTransactions> enrichedTrxStream = transactionsCdcStream
				.connect(customersCdcStream)
				.process(join1)
				.uid("enriched_transactions")
				.keyBy(destinationAccountKeySelector)
				.connect(customersCdcStream)
				.process(join2)
				.uid("enriched_transactions_2");

		SingleOutputStreamOperator<String> parsedStream = enrichedTrxStream
				.map(enrichedTransactionsParser);

		enrichedTrxStream.addSink(etxKafkaProducer);

		parsedStream.print();

		// execute program
		env.execute("Flink Streaming PostgreSQL CDC");
	}
}
