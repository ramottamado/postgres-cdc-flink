package dev.ramottamado.java.flink.util.serialization;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class JSONValueDeserializationSchema implements DeserializationSchema<ObjectNode> {

    private static final long serialVersionUID = -91238719810201L;
    private ObjectMapper mapper;

    @Override
    public ObjectNode deserialize(byte[] message) throws IOException {

        if (mapper == null) {
            mapper = new ObjectMapper();
        }

        ObjectNode node = mapper.createObjectNode();

        if (message != null) {
            node.set("value", mapper.readValue(message, JsonNode.class));
        }

        return node;
    }

    @Override
    public boolean isEndOfStream(ObjectNode nextElement) {

        return false; // Unbounded stream
    }

    @Override
    public TypeInformation<ObjectNode> getProducedType() {

        return getForClass(ObjectNode.class);
    }
}
