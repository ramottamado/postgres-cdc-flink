package dev.ramottamado.java.flink;

import static dev.ramottamado.java.flink.util.ParameterConstants.KAFKA_SOURCE_TOPIC;

import java.util.Properties;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

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

		DataStream<String> parsedStream = postgresCdcStream.map(record -> {
			try {
				return record.get("value").get("payload").get("after").get("name").asText();
			} catch (Exception e) {
				e.printStackTrace();
				return "";
			}
		});

		parsedStream.print();

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
