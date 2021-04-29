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

package dev.ramottamado.java.flink.util.serialization;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class JSONValueDeserializationSchemaTest {
    private final JSONValueDeserializationSchema jsonValueDeserializationSchemaTest =
            new JSONValueDeserializationSchema();
    private final ObjectMapper mapper = new ObjectMapper();
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
        ObjectNode actual = jsonValueDeserializationSchemaTest.deserialize(message);
        ObjectNode expected = mapper.createObjectNode().set("value", mapper.readValue(dummyEnvelope, JsonNode.class));

        Assert.assertNotNull(actual);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testDeserializeNull() throws Exception {
        ObjectNode actual = jsonValueDeserializationSchemaTest.deserialize(null);
        ObjectNode expected = mapper.createObjectNode();

        Assert.assertNotNull(actual);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testIsEndOfStream() {
        ObjectNode nextElement = mapper.createObjectNode();
        boolean actual = jsonValueDeserializationSchemaTest.isEndOfStream(nextElement);

        Assert.assertFalse(actual);
    }

    @Test
    public void testGetProducedType() {
        TypeInformation<ObjectNode> actual = jsonValueDeserializationSchemaTest.getProducedType();

        Assert.assertNotNull(actual);
        Assert.assertEquals(TypeInformation.of(ObjectNode.class), actual);
    }
}
