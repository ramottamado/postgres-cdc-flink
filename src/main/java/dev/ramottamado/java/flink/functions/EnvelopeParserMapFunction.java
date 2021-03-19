package dev.ramottamado.java.flink.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class EnvelopeParserMapFunction<T> implements MapFunction<ObjectNode, T> {

    public EnvelopeParserMapFunction(Class<T> type) {
        this.type = type;
    }

    private final Class<T> type;

    private static final long serialVersionUID = 123456672L;

    private ObjectMapper mapper;

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
