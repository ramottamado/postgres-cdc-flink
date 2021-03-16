package dev.ramottamado.java.flink;

import static dev.ramottamado.java.flink.config.ParameterConfig.KAFKA_SOURCE_TOPIC;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import dev.ramottamado.java.flink.functions.TransactionsEnvelopeParserMapFunction;
import dev.ramottamado.java.flink.schema.Transactions;
import dev.ramottamado.java.flink.util.ParameterUtils;
import dev.ramottamado.java.flink.util.kafka.KafkaProperties;
import dev.ramottamado.java.flink.util.serialization.JSONValueDeserializationSchema;

/**
 * Stream PostgreSQL CDC from Debezium into Flink.
 */
public class PostgresCdcStreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		ParameterTool params = ParameterUtils.parseArgs(args);
		Properties properties = KafkaProperties.getProperties(params);

		DataStream<ObjectNode> postgresCdcStream = env.addSource(new FlinkKafkaConsumer<>(
				params.getRequired(KAFKA_SOURCE_TOPIC), new JSONValueDeserializationSchema(), properties));

		DataStream<Transactions> usersStream = postgresCdcStream.map(new TransactionsEnvelopeParserMapFunction());

		DataStream<JsonNode> parsedStream = usersStream.map(new MapFunction<Transactions, JsonNode>() {

			private final static long serialVersionUID = -129393123132L;

			ObjectMapper mapper = new ObjectMapper();

			@Override
			public JsonNode map(Transactions value) throws Exception {
				return mapper.valueToTree(value);
			}
		});

		// postgresCdcStream.print();

		parsedStream.print();

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
