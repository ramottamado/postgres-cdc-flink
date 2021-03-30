package dev.ramottamado.java.flink.functions;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Assert;
import org.junit.Test;

import dev.ramottamado.java.flink.util.serialization.JSONValueDeserializationSchema;

public class JSONValueDeserializationSchemaTest {
    private JSONValueDeserializationSchema jsonValueDeserializationSchemaTest;
    private ObjectMapper mapper;
    private String dummyEnvelope;
    private byte[] message;

    public void prepareTest() throws Exception {
        jsonValueDeserializationSchemaTest = new JSONValueDeserializationSchema();

        dummyEnvelope = String.join(System.getProperty("line.separator"),
                "{",
                "   \"schema\" : {},",
                "   \"payload\" : {",
                "       \"op\": \"i\",",
                "       \"source\": {},",
                "       \"ts_ms\" : \"\",",
                "       \"before\" : {",
                "       },",
                "       \"after\" : {",
                "          \"cif\" : \"029817127819\",",
                "          \"acct_number\" : \"067637881\",",
                "          \"first_name\" : \"Taufiq\",",
                "          \"last_name\" : \"Maulana\",",
                "          \"city\" : \"Jakarta\"",
                "       }",
                "   }",
                "}");

        mapper = new ObjectMapper();
        message = dummyEnvelope.getBytes();
    }

    @Test
    public void testDeserialize() throws Exception {
        prepareTest();
        ObjectNode out = jsonValueDeserializationSchemaTest.deserialize(message);
        ObjectNode expected = mapper.createObjectNode().set("value", mapper.readValue(dummyEnvelope, JsonNode.class));

        Assert.assertNotNull(out);
        Assert.assertEquals(expected, out);
    }

    @Test
    public void testDeserializeNull() throws Exception {
        prepareTest();
        message = null;
        ObjectNode out = jsonValueDeserializationSchemaTest.deserialize(message);
        ObjectNode expected = mapper.createObjectNode();

        Assert.assertNotNull(out);
        Assert.assertEquals(expected, out);
    }

    @Test
    public void testIsEndOfStream() throws Exception {
        prepareTest();
        ObjectNode nextElement = mapper.createObjectNode();
        boolean out = jsonValueDeserializationSchemaTest.isEndOfStream(nextElement);

        Assert.assertNotNull(out);
        Assert.assertEquals(false, out);
    }

    @Test
    public void testGetProducedType() throws Exception {
        prepareTest();
        TypeInformation<ObjectNode> out = jsonValueDeserializationSchemaTest.getProducedType();

        Assert.assertNotNull(out);
        Assert.assertEquals(out, TypeInformation.of(ObjectNode.class));
    }
}
