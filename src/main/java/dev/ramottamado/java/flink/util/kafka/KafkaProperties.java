package dev.ramottamado.java.flink.util.kafka;

import static dev.ramottamado.java.flink.config.ParameterConfig.KAFKA_AUTO_OFFSET_RESET;
import static dev.ramottamado.java.flink.config.ParameterConfig.KAFKA_BOOTSTRAP_SERVER;

import java.text.MessageFormat;
import java.util.Properties;
import java.util.UUID;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProperties {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProperties.class);

    public static final Properties getProperties(ParameterTool params) throws RuntimeException {

        logger.info("Getting properties");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", params.getRequired(KAFKA_BOOTSTRAP_SERVER));
        properties.setProperty("group.id", MessageFormat.format("flink_consumer_{0}", UUID.randomUUID())); // TODO:
                                                                                                           // Replace
                                                                                                           // with
                                                                                                           // actual
                                                                                                           // group id
        properties.setProperty("auto.offset.reset", params.get(KAFKA_AUTO_OFFSET_RESET));

        return properties;
    }
}
