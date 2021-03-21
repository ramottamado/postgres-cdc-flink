package dev.ramottamado.java.flink.config;

/**
 * The main class to place constants for {@link org.apache.flink.api.java.utils.ParameterTool}.
 * The constants will be the parameters when running the application.
 */
public final class ParameterConfig {
    public static final String CHECKPOINT_PATH = "checkpoint-path";
    public static final String DEBUG_RESULT_STREAM = "debug-result-stream";
    public static final String ENVIRONMENT = "environment";
    public static final String KAFKA_AUTO_OFFSET_RESET = "auto-offset-reset";
    public static final String KAFKA_BOOTSTRAP_SERVER = "bootstrap-server";
    public static final String KAFKA_CONSUMER_GROUP_ID = "consumer-group-id";
    public static final String KAFKA_OFFSET_STRATEGY = "offset-strategy";
    public static final String KAFKA_SOURCE_TOPIC_1 = "source-topic-1";
    public static final String KAFKA_SOURCE_TOPIC_2 = "source-topic-2";
    public static final String KAFKA_TARGET_TOPIC = "target-topic";
    public static final String PROPERTIES_FILE = "properties-file";

    private ParameterConfig() {
    }
}
