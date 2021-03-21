package dev.ramottamado.java.flink.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The {@link EnrichedTransactionsToStringMapFunction} implements
 * {@link org.apache.flink.api.common.functions.MapFunction}
 * allows for deserializing Debezium JSON envelope into POJO.
 *
 * @param <T> the type of deserialized POJO
 * @see       dev.ramottamado.java.flink.KafkaTransactionsEnrichmentStreamingJob#writeEnrichedTransactionsOutput(
 *            org.apache.flink.streaming.api.datastream.DataStream,
 *            org.apache.flink.api.java.utils.ParameterTool)
 */
public class EnvelopeParserMapFunction<T> implements MapFunction<ObjectNode, T> {
    private static final long serialVersionUID = 123456672L;
    private final Class<T> type;
    private ObjectMapper mapper;

    /**
     * The {@link EnrichedTransactionsToStringMapFunction} implements
     * {@link org.apache.flink.api.common.functions.MapFunction}
     * allows for deserializing Debezium JSON envelope into POJO.
     *
     * @param type the type of deserialized POJO
     */
    public EnvelopeParserMapFunction(Class<T> type) {
        this.type = type;
    }

    @Override
    public T map(ObjectNode value) throws Exception {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }

        try {
            return mapper.treeToValue(value.get("value").get("payload").get("after"), type);
        } catch (Exception e) {
            return type.newInstance();
        }
    }
}
