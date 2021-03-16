package dev.ramottamado.java.flink.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class EnvelopeParserMapFunction<T> implements MapFunction<ObjectNode, T> {

    private static final long serialVersionUID = 123456672L;

    private final Class<T> gen;

    public EnvelopeParserMapFunction(Class<T> gen) {
        this.gen = gen;
    }

    @Override
    public T map(ObjectNode value) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        try {
            return mapper.treeToValue(value.get("value").get("payload").get("after"), gen);
        } catch (Exception e) {
            return gen.newInstance();
        }
    }

}
