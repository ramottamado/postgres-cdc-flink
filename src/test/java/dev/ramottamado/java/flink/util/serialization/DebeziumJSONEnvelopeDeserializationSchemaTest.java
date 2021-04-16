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

import dev.ramottamado.java.flink.schema.Customer;
import dev.ramottamado.java.flink.schema.Transaction;

public class DebeziumJSONEnvelopeDeserializationSchemaTest {
    private DebeziumJSONEnvelopeDeserializationSchema<?> debeziumJSONEnvelopeDeserializationSchema;
    private Customer cust;
    private Transaction trx;
    private String dummyCustEnvelope;
    private String dummyTrxEnvelope;
    private byte[] message;

    @Before
    public void prepareTest() {
        cust = new Customer();
        cust.setAcctNumber("0001");
        cust.setCif("001");
        cust.setCity("Bandung");
        cust.setFirstName("Tamado");
        cust.setLastName("Sitohang");

        trx = new Transaction();
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
        debeziumJSONEnvelopeDeserializationSchema = new DebeziumJSONEnvelopeDeserializationSchema<>(Customer.class);
        message = dummyCustEnvelope.getBytes();

        Customer actual = (Customer) debeziumJSONEnvelopeDeserializationSchema.deserialize(message);

        Assert.assertNotNull(actual);
        Assert.assertEquals(cust, actual);
    }

    @Test
    public void testDeserializeTransaction() throws Exception {
        debeziumJSONEnvelopeDeserializationSchema = new DebeziumJSONEnvelopeDeserializationSchema<>(Transaction.class);
        message = dummyTrxEnvelope.getBytes();

        Transaction actual = (Transaction) debeziumJSONEnvelopeDeserializationSchema.deserialize(message);

        Assert.assertNotNull(actual);
        Assert.assertEquals(trx, actual);
    }

    @Test
    public void testDeserializeNull() throws Exception {
        debeziumJSONEnvelopeDeserializationSchema = new DebeziumJSONEnvelopeDeserializationSchema<>(Transaction.class);

        Transaction actual = (Transaction) debeziumJSONEnvelopeDeserializationSchema.deserialize(null);

        Assert.assertNotNull(actual);
        Assert.assertNull(actual.getSrcAcct());
        Assert.assertNull(actual.getDestAcct());
        Assert.assertNull(actual.getTrxType());
        Assert.assertNull(actual.getAmount());
        Assert.assertNull(actual.getTrxTimestamp());
    }
}
