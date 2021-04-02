/*
 * Copyright 2021 Tamado Sitohang <ramot@ramottamado.dev>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.ramottamado.java.flink.functions;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import dev.ramottamado.java.flink.util.serialization.JSONValueDeserializationSchema;

public class JSONValueDeserializationSchemaTest {
    private JSONValueDeserializationSchema jsonValueDeserializationSchemaTest = new JSONValueDeserializationSchema();
    private ObjectMapper mapper = new ObjectMapper();
    private static String dummyEnvelope;
    private static byte[] message;

    @BeforeClass
    public static void prepareTest() throws Exception {
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

        message = dummyEnvelope.getBytes();
    }

    @Test
    public void testDeserialize() throws Exception {
        ObjectNode out = jsonValueDeserializationSchemaTest.deserialize(message);
        ObjectNode expected = mapper.createObjectNode().set("value", mapper.readValue(dummyEnvelope, JsonNode.class));

        Assert.assertNotNull(out);
        Assert.assertEquals(expected, out);
    }

    @Test
    public void testDeserializeNull() throws Exception {
        message = null;
        ObjectNode out = jsonValueDeserializationSchemaTest.deserialize(message);
        ObjectNode expected = mapper.createObjectNode();

        Assert.assertNotNull(out);
        Assert.assertEquals(expected, out);
    }

    @Test
    public void testIsEndOfStream() throws Exception {
        ObjectNode nextElement = mapper.createObjectNode();
        boolean out = jsonValueDeserializationSchemaTest.isEndOfStream(nextElement);

        Assert.assertNotNull(out);
        Assert.assertEquals(false, out);
    }

    @Test
    public void testGetProducedType() throws Exception {
        TypeInformation<ObjectNode> out = jsonValueDeserializationSchemaTest.getProducedType();

        Assert.assertNotNull(out);
        Assert.assertEquals(out, TypeInformation.of(ObjectNode.class));
    }
}
