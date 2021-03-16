package dev.ramottamado.java.flink.config;

public final class ParameterConfig {
    public final static String KAFKA_AUTO_OFFSET_RESET = "offset-reset";
    public final static String KAFKA_BOOTSTRAP_SERVER = "bootstrap-server";
    public final static String KAFKA_SOURCE_TOPIC = "source-topic";
    public final static String KAFKA_TARGET_TOPIC = "target-topic";
    public final static String PROPERTIES_FILE = "properties";

    private ParameterConfig() {
    }
}
