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

import java.time.Instant;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import dev.ramottamado.java.flink.schema.CustomersBean;
import dev.ramottamado.java.flink.schema.EnrichedTransactionsBean;

public class EnrichEnrichedTransactionsWithCustomersJoinFunctionTest {
    private EnrichEnrichedTransactionsWithCustomersJoinFunction enrichEnrichedTransactionsWithCustomersJoinFunction;
    private KeyedTwoInputStreamOperatorTestHarness<String, EnrichedTransactionsBean, CustomersBean, EnrichedTransactionsBean> testHarness;
    private CustomersBean testCustomer;
    private EnrichedTransactionsBean testEnrichedTrx;
    private EnrichedTransactionsBean testEnrichedTrxResult;

    @Before
    public void prepareTest() {
        testCustomer = new CustomersBean();
        testCustomer.setAcctNumber("0002");
        testCustomer.setCif("002");
        testCustomer.setCity("Jakarta");
        testCustomer.setFirstName("Kamal");
        testCustomer.setLastName("Rasyid");

        testEnrichedTrx = new EnrichedTransactionsBean();
        testEnrichedTrx.setAmount(10000.0);
        testEnrichedTrx.setDestAcct("0002");
        testEnrichedTrx.setSrcAcct("0001");
        testEnrichedTrx.setTrxTimestamp(Instant.parse("2021-01-01T12:00:00.00Z"));
        testEnrichedTrx.setTrxType("TRANSFER");
        testEnrichedTrx.setCif("001");
        testEnrichedTrx.setSrcName("Tamado Sitohang");

        testEnrichedTrxResult = new EnrichedTransactionsBean();
        testEnrichedTrxResult.setAmount(10000.0);
        testEnrichedTrxResult.setDestAcct("0002");
        testEnrichedTrxResult.setSrcAcct("0001");
        testEnrichedTrxResult.setTrxTimestamp(Instant.parse("2021-01-01T12:00:00.00Z"));
        testEnrichedTrxResult.setTrxType("TRANSFER");
        testEnrichedTrxResult.setCif("001");
        testEnrichedTrxResult.setSrcName("Tamado Sitohang");
        testEnrichedTrxResult.setDestName("Kamal Rasyid");
    }

    @Test
    public void testProcessElement() throws Exception {
        enrichEnrichedTransactionsWithCustomersJoinFunction = new EnrichEnrichedTransactionsWithCustomersJoinFunction();

        testHarness = new KeyedTwoInputStreamOperatorTestHarness<>(
                new KeyedCoProcessOperator<>(enrichEnrichedTransactionsWithCustomersJoinFunction),
                EnrichedTransactionsBean::getDestAcctAsKey,
                CustomersBean::getAcctNumber,
                Types.STRING);

        testHarness.open();
        testHarness.processElement2(testCustomer, 10);
        testHarness.processElement1(testEnrichedTrx, 10);

        for (StreamRecord<? extends EnrichedTransactionsBean> etx : testHarness.extractOutputStreamRecords()) {
            Assert.assertEquals(testEnrichedTrxResult.getCif(), etx.getValue().getCif());
            Assert.assertEquals(testEnrichedTrxResult.getDestAcct(), etx.getValue().getDestAcct());
            Assert.assertEquals(testEnrichedTrxResult.getSrcAcct(), etx.getValue().getSrcAcct());
            Assert.assertEquals(testEnrichedTrxResult.getSrcName(), etx.getValue().getSrcName());
            Assert.assertEquals(testEnrichedTrxResult.getDestName(), etx.getValue().getDestName());
            Assert.assertEquals(testEnrichedTrxResult.getTrxType(), etx.getValue().getTrxType());
            Assert.assertEquals(testEnrichedTrxResult.getAmount(), etx.getValue().getAmount());
        }
    }

    @Test
    public void testProcessElementWithNullCustomer() throws Exception {
        enrichEnrichedTransactionsWithCustomersJoinFunction = new EnrichEnrichedTransactionsWithCustomersJoinFunction();

        testEnrichedTrx.setDestAcct(null);

        testHarness = new KeyedTwoInputStreamOperatorTestHarness<>(
                new KeyedCoProcessOperator<>(enrichEnrichedTransactionsWithCustomersJoinFunction),
                EnrichedTransactionsBean::getDestAcctAsKey,
                CustomersBean::getAcctNumber,
                Types.STRING);

        testHarness.open();
        testHarness.processElement2(testCustomer, 10);
        testHarness.processElement1(testEnrichedTrx, 10);

        for (StreamRecord<? extends EnrichedTransactionsBean> etx : testHarness.extractOutputStreamRecords()) {
            Assert.assertEquals(testEnrichedTrxResult.getCif(), etx.getValue().getCif());
            Assert.assertEquals(null, etx.getValue().getDestAcct());
            Assert.assertEquals(testEnrichedTrxResult.getSrcAcct(), etx.getValue().getSrcAcct());
            Assert.assertEquals(testEnrichedTrxResult.getSrcName(), etx.getValue().getSrcName());
            Assert.assertEquals(null, etx.getValue().getDestName());
            Assert.assertEquals(testEnrichedTrxResult.getTrxType(), etx.getValue().getTrxType());
            Assert.assertEquals(testEnrichedTrxResult.getAmount(), etx.getValue().getAmount());
        }
    }

    @Test
    public void testOnTimer() throws Exception {
        enrichEnrichedTransactionsWithCustomersJoinFunction = new EnrichEnrichedTransactionsWithCustomersJoinFunction();

        testHarness = new KeyedTwoInputStreamOperatorTestHarness<>(
                new KeyedCoProcessOperator<>(enrichEnrichedTransactionsWithCustomersJoinFunction),
                EnrichedTransactionsBean::getDestAcctAsKey,
                CustomersBean::getAcctNumber,
                Types.STRING);

        testHarness.open();
        testHarness.processElement1(testEnrichedTrx, 10);
        testHarness.processElement2(testCustomer, 10);
        testHarness.setProcessingTime(15000);

        for (StreamRecord<? extends EnrichedTransactionsBean> etx : testHarness.extractOutputStreamRecords()) {
            Assert.assertEquals(testEnrichedTrxResult.getCif(), etx.getValue().getCif());
            Assert.assertEquals(testEnrichedTrxResult.getDestAcct(), etx.getValue().getDestAcct());
            Assert.assertEquals(testEnrichedTrxResult.getSrcAcct(), etx.getValue().getSrcAcct());
            Assert.assertEquals(testEnrichedTrxResult.getSrcName(), etx.getValue().getSrcName());
            Assert.assertEquals(testEnrichedTrxResult.getDestName(), etx.getValue().getDestName());
            Assert.assertEquals(testEnrichedTrxResult.getTrxType(), etx.getValue().getTrxType());
            Assert.assertEquals(testEnrichedTrxResult.getAmount(), etx.getValue().getAmount());
        }
    }

    @Test
    public void testOnTimerWithNullCustomer() throws Exception {
        enrichEnrichedTransactionsWithCustomersJoinFunction = new EnrichEnrichedTransactionsWithCustomersJoinFunction();

        testHarness = new KeyedTwoInputStreamOperatorTestHarness<>(
                new KeyedCoProcessOperator<>(enrichEnrichedTransactionsWithCustomersJoinFunction),
                EnrichedTransactionsBean::getDestAcctAsKey,
                CustomersBean::getAcctNumber,
                Types.STRING);

        testHarness.open();
        testHarness.processElement1(testEnrichedTrx, 10);
        testHarness.setProcessingTime(5011);

        for (StreamRecord<? extends EnrichedTransactionsBean> etx : testHarness.extractOutputStreamRecords()) {
            Assert.assertEquals(testEnrichedTrxResult.getCif(), etx.getValue().getCif());
            Assert.assertEquals(testEnrichedTrxResult.getDestAcct(), etx.getValue().getDestAcct());
            Assert.assertEquals(testEnrichedTrxResult.getSrcAcct(), etx.getValue().getSrcAcct());
            Assert.assertEquals(testEnrichedTrxResult.getSrcName(), etx.getValue().getSrcName());
            Assert.assertEquals(null, etx.getValue().getDestName());
            Assert.assertEquals(testEnrichedTrxResult.getTrxType(), etx.getValue().getTrxType());
            Assert.assertEquals(testEnrichedTrxResult.getAmount(), etx.getValue().getAmount());
        }
    }
}
