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

import java.time.Instant;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import dev.ramottamado.java.flink.schema.CustomersBean;
import dev.ramottamado.java.flink.schema.TransactionsBean;

public class DebeziumJSONEnvelopeDeserializationSchemaTest {
    private DebeziumJSONEnvelopeDeserializationSchema<?> debeziumJSONEnvelopeDeserializationSchema;
    private CustomersBean cust;
    private TransactionsBean trx;
    private String dummyCustEnvelope;
    private String dummyTrxEnvelope;
    private byte[] message;

    @Before
    public void prepareTest() {
        cust = new CustomersBean();
        cust.setAcctNumber("0001");
        cust.setCif("001");
        cust.setCity("Bandung");
        cust.setFirstName("Tamado");
        cust.setLastName("Sitohang");

        trx = new TransactionsBean();
        trx.setAmount(10000.0);
        trx.setDestAcct("0002");
        trx.setSrcAcct("0001");
        trx.setTrxTimestamp(Instant.parse("2021-01-01T12:00:00.00Z"));
        trx.setTrxType("TRANSFER");

        dummyCustEnvelope = String.join(System.getProperty("line.separator"),
                "{",
                "   \"schema\" : {},",
                "   \"payload\" : {",
                "       \"op\": \"i\",",
                "       \"source\": {},",
                "       \"ts_ms\" : \"\",",
                "       \"before\" : {",
                "       },",
                "       \"after\" : {",
                "          \"cif\" : \"001\",",
                "          \"acct_number\" : \"0001\",",
                "          \"first_name\" : \"Tamado\",",
                "          \"last_name\" : \"Sitohang\",",
                "          \"city\" : \"Bandung\"",
                "       }",
                "   }",
                "}");

        dummyTrxEnvelope = String.join(System.getProperty("line.separator"),
                "{",
                "   \"schema\" : {},",
                "   \"payload\" : {",
                "       \"op\": \"i\",",
                "       \"source\": {},",
                "       \"ts_ms\" : \"\",",
                "       \"before\" : {",
                "       },",
                "       \"after\" : {",
                "          \"amount\" : 10000.0,",
                "          \"src_acct\" : \"0001\",",
                "          \"dest_acct\" : \"0002\",",
                "          \"trx_timestamp\" : 1609502400000000,",
                "          \"trx_type\" : \"TRANSFER\"",
                "       }",
                "   }",
                "}");
    }

    @Test
    public void testDeserializeCustomer() throws Exception {
        debeziumJSONEnvelopeDeserializationSchema =
                new DebeziumJSONEnvelopeDeserializationSchema<CustomersBean>(CustomersBean.class);

        message = dummyCustEnvelope.getBytes();

        CustomersBean out = (CustomersBean) debeziumJSONEnvelopeDeserializationSchema.deserialize(message);

        Assert.assertNotNull(out);
        Assert.assertEquals(cust.getAcctNumber(), out.getAcctNumber());
        Assert.assertEquals(cust.getCif(), out.getCif());
        Assert.assertEquals(cust.getCity(), out.getCity());
        Assert.assertEquals(cust.getFirstName(), out.getFirstName());
        Assert.assertEquals(cust.getLastName(), out.getLastName());
    }

    @Test
    public void testDeserializeTransaction() throws Exception {
        debeziumJSONEnvelopeDeserializationSchema =
                new DebeziumJSONEnvelopeDeserializationSchema<TransactionsBean>(TransactionsBean.class);

        message = dummyTrxEnvelope.getBytes();

        TransactionsBean out = (TransactionsBean) debeziumJSONEnvelopeDeserializationSchema.deserialize(message);

        Assert.assertNotNull(out);
        Assert.assertEquals(trx.getSrcAcct(), out.getSrcAcct());
        Assert.assertEquals(trx.getDestAcct(), out.getDestAcct());
        Assert.assertEquals(trx.getTrxType(), out.getTrxType());
        Assert.assertEquals(trx.getTrxTimestamp(), out.getTrxTimestamp());
        Assert.assertEquals(trx.getAmount(), out.getAmount());
    }

    @Test
    public void testDeserializeNull() throws Exception {
        message = null;

        debeziumJSONEnvelopeDeserializationSchema =
                new DebeziumJSONEnvelopeDeserializationSchema<TransactionsBean>(TransactionsBean.class);

        TransactionsBean out = (TransactionsBean) debeziumJSONEnvelopeDeserializationSchema.deserialize(message);

        Assert.assertNotNull(out);
        Assert.assertNull(out.getSrcAcct());
        Assert.assertNull(out.getDestAcct());
        Assert.assertNull(out.getTrxType());
        Assert.assertNull(out.getAmount());
        Assert.assertNull(out.getTrxTimestamp());
    }
}
