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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import dev.ramottamado.java.flink.schema.CustomersBean;

public class EnvelopeParserMapFunctionTest {
    private ObjectMapper mapper = new ObjectMapper();
    private static String dummyEnvelope;
    private static CustomersBean expected;

    @BeforeClass
    public static void prepareTest() {
        expected = new CustomersBean();
        expected.setCif("029817127819");
        expected.setAcctNumber("067637881");
        expected.setCity("Jakarta");
        expected.setFirstName("Taufiq");
        expected.setLastName("Maulana");

        dummyEnvelope = String.join("\n",
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
    }

    @Test
    public void testMap() throws Exception {
        ObjectNode dummyObjectNode = mapper.createObjectNode();
        dummyObjectNode.set("value", mapper.readTree(dummyEnvelope));

        EnvelopeParserMapFunction<CustomersBean> customersEnvelopeParserMapFunction =
                new EnvelopeParserMapFunction<>(CustomersBean.class);
        CustomersBean out = customersEnvelopeParserMapFunction.map(dummyObjectNode);

        Assert.assertNotNull(out);
        Assert.assertEquals(expected.getAcctNumber(), out.getAcctNumber());
        Assert.assertEquals(expected.getCif(), out.getCif());
        Assert.assertEquals(expected.getCity(), out.getCity());
        Assert.assertEquals(expected.getFirstName(), out.getFirstName());
        Assert.assertEquals(expected.getLastName(), out.getLastName());
    }
}
