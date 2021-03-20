package dev.ramottamado.java.flink.util.serialization;

import java.io.IOException;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class DebeziumJSONEnvelopeDeserializationSchema<T> extends AbstractDeserializationSchema<T> {

    private static final long serialVersionUID = -91238719810201L;
    private ObjectMapper mapper;

    public DebeziumJSONEnvelopeDeserializationSchema(Class<T> type) {

        super(type);
    }

    @Override
    public T deserialize(byte[] message) throws IOException {

        if (mapper == null) {
            mapper = new ObjectMapper();
        }

        ObjectNode node = mapper.createObjectNode();

        if (message != null) {
            node.set("value", mapper.readValue(message, JsonNode.class));
        }

        try {
            return mapper.treeToValue(
                    node.get("value").get("payload").get("after"),
                    super.getProducedType().getTypeClass()
            );
        } catch (Exception e) {

            try {
                return super.getProducedType().getTypeClass().newInstance();
            } catch (InstantiationException | IllegalAccessException e1) {
                throw new IOException("Error instantiating new class");
            }
        }
    }
}
