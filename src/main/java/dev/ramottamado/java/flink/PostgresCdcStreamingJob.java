package dev.ramottamado.java.flink;

import static dev.ramottamado.java.flink.config.ParameterConfig.KAFKA_SOURCE_TOPIC_1;
import static dev.ramottamado.java.flink.config.ParameterConfig.KAFKA_SOURCE_TOPIC_2;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import dev.ramottamado.java.flink.functions.DestinationAccountKeySelector;
import dev.ramottamado.java.flink.functions.EnrichEnrichedTransactionsWithCustomersJoinFunction;
import dev.ramottamado.java.flink.functions.EnrichTransactionsWithCustomersJoinFunction;
import dev.ramottamado.java.flink.functions.EnrichedTransactionsToStringMapFunction;
import dev.ramottamado.java.flink.functions.EnvelopeParserMapFunction;
import dev.ramottamado.java.flink.schema.Customers;
import dev.ramottamado.java.flink.schema.EnrichedTransactions;
import dev.ramottamado.java.flink.schema.Transactions;
import dev.ramottamado.java.flink.util.ParameterUtils;
import dev.ramottamado.java.flink.util.kafka.KafkaProperties;
import dev.ramottamado.java.flink.util.serialization.JSONValueDeserializationSchema;

/**
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
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

		env.setParallelism(2);
		env.enableCheckpointing(10000L);

		CheckpointConfig config = env.getCheckpointConfig();
		config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

		DataStream<Transactions> transactionsCdcStream = env
				.addSource(new FlinkKafkaConsumer<>(params.getRequired(KAFKA_SOURCE_TOPIC_1),
						new JSONValueDeserializationSchema(), properties))
				.map(new EnvelopeParserMapFunction<>(Transactions.class)).returns(Transactions.class)
				.keyBy(x -> x.getSrcAccount());

		DataStream<Customers> customersCdcStream = env
				.addSource(new FlinkKafkaConsumer<>(params.getRequired(KAFKA_SOURCE_TOPIC_2),
						new JSONValueDeserializationSchema(), properties))
				.map(new EnvelopeParserMapFunction<>(Customers.class)).returns(Customers.class)
				.keyBy(x -> x.getAcctNumber());

		DataStream<EnrichedTransactions> enrichedTrxStream = transactionsCdcStream.connect(customersCdcStream)
				.process(new EnrichTransactionsWithCustomersJoinFunction()).uid("enrich");

		DataStream<EnrichedTransactions> enrichedTrxStream2 = enrichedTrxStream
				.keyBy(new DestinationAccountKeySelector()).connect(customersCdcStream)
				.process(new EnrichEnrichedTransactionsWithCustomersJoinFunction()).uid("enrich2");

		DataStream<String> parsedStream2 = enrichedTrxStream2.map(new EnrichedTransactionsToStringMapFunction());

		// postgresCdcStream.print();

		// parsedStream.print();
		parsedStream2.print();

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
