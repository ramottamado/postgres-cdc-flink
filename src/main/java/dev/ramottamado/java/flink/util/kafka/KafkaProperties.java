package dev.ramottamado.java.flink.util.kafka;

import static dev.ramottamado.java.flink.config.ParameterConfig.KAFKA_AUTO_OFFSET_RESET;
import static dev.ramottamado.java.flink.config.ParameterConfig.KAFKA_BOOTSTRAP_SERVER;
import static dev.ramottamado.java.flink.config.ParameterConfig.KAFKA_CONSUMER_GROUP_ID;

import java.util.Properties;

import org.apache.flink.api.java.utils.ParameterTool;

/**
 * The {@link KafkaProperties} allows for constructing new {@link Properties} for Kafka consumer from
 * {@link ParameterTool}.
 */
public class KafkaProperties {
    /**
     * Get new {@link Properties} from passed {@link ParameterTool}.
     *
     * @param  params           the parameters inside {@link ParameterTool}
     * @return                  the properties for Kafka consumer to use
     * @throws RuntimeException if {@link KAFKA_BOOTSTRAP_SERVER} is not set
     */
    public static final Properties getProperties(ParameterTool params) throws RuntimeException {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", params.getRequired(KAFKA_BOOTSTRAP_SERVER));
        properties.setProperty("group.id", params.get(KAFKA_CONSUMER_GROUP_ID, "flink-cdc-consumer"));
        properties.setProperty("auto.offset.reset", params.get(KAFKA_AUTO_OFFSET_RESET, "latest"));

        return properties;
    }
}
