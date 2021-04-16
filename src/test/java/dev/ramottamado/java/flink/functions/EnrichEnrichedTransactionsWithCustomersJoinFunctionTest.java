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
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.junit.Before;
import org.junit.Test;

import dev.ramottamado.java.flink.schema.Customer;
import dev.ramottamado.java.flink.schema.EnrichedTransaction;

public class EnrichEnrichedTransactionsWithCustomersJoinFunctionTest {
    private KeyedTwoInputStreamOperatorTestHarness<String, EnrichedTransaction, Customer, EnrichedTransaction> testHarness;
    private Customer testCustomer;
    private EnrichedTransaction testEnrichedTrx;
    private EnrichedTransaction testEnrichedTrxResult;

    @Before
    public void prepareTest() throws Exception {
        testCustomer = new Customer();
        testCustomer.setAcctNumber("0002");
        testCustomer.setCif("002");
        testCustomer.setCity("Jakarta");
        testCustomer.setFirstName("Kamal");
        testCustomer.setLastName("Rasyid");

        testEnrichedTrx = new EnrichedTransaction();
        testEnrichedTrx.setAmount(10000.0);
        testEnrichedTrx.setDestAcct("0002");
        testEnrichedTrx.setSrcAcct("0001");
        testEnrichedTrx.setTrxTimestamp(Instant.parse("2021-01-01T12:00:00.00Z"));
        testEnrichedTrx.setTrxType("TRANSFER");
        testEnrichedTrx.setCif("001");
        testEnrichedTrx.setSrcName("Tamado Sitohang");

        testEnrichedTrxResult = new EnrichedTransaction();
        testEnrichedTrxResult.setAmount(10000.0);
        testEnrichedTrxResult.setDestAcct("0002");
        testEnrichedTrxResult.setSrcAcct("0001");
        testEnrichedTrxResult.setTrxTimestamp(Instant.parse("2021-01-01T12:00:00.00Z"));
        testEnrichedTrxResult.setTrxType("TRANSFER");
        testEnrichedTrxResult.setCif("001");
        testEnrichedTrxResult.setSrcName("Tamado Sitohang");
        testEnrichedTrxResult.setDestName("Kamal Rasyid");

        EnrichEnrichedTransactionsWithCustomersJoinFunction enrichEnrichedTransactionsWithCustomersJoinFunction =
                new EnrichEnrichedTransactionsWithCustomersJoinFunction();

        testHarness = new KeyedTwoInputStreamOperatorTestHarness<>(
                new KeyedCoProcessOperator<>(enrichEnrichedTransactionsWithCustomersJoinFunction),
                EnrichedTransaction::getDestAcctAsKey,
                Customer::getAcctNumber,
                Types.STRING);

        testHarness.open();
    }

    @Test
    public void testProcessElement() throws Exception {
        testHarness.processElement2(testCustomer, 10);
        testHarness.processElement1(testEnrichedTrx, 10);

        ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();
        expected.add(new StreamRecord<>(testEnrichedTrxResult, 10));

        ConcurrentLinkedQueue<Object> actual = testHarness.getOutput();

        TestHarnessUtil.assertOutputEquals("Output not as expected.", expected, actual);
    }

    @Test
    public void testProcessElementWithNullCustomer() throws Exception {
        testEnrichedTrx.setDestAcct(null);
        testEnrichedTrxResult.setDestAcct(null);
        testEnrichedTrxResult.setDestName(null);

        testHarness.processElement2(testCustomer, 10);
        testHarness.processElement1(testEnrichedTrx, 10);

        ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();
        expected.add(new StreamRecord<>(testEnrichedTrxResult, 10));

        ConcurrentLinkedQueue<Object> actual = testHarness.getOutput();

        TestHarnessUtil.assertOutputEquals("Output not as expected.", expected, actual);
    }

    @Test
    public void testOnTimer() throws Exception {
        testHarness.processElement1(testEnrichedTrx, 10);
        testHarness.processElement2(testCustomer, 10);
        testHarness.setProcessingTime(15000);

        ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();
        expected.add(new StreamRecord<>(testEnrichedTrxResult));

        ConcurrentLinkedQueue<Object> actual = testHarness.getOutput();

        TestHarnessUtil.assertOutputEquals("Output not as expected.", expected, actual);
    }

    @Test
    public void testOnTimerWithNullCustomer() throws Exception {
        testEnrichedTrxResult.setDestName(null);

        testHarness.processElement1(testEnrichedTrx, 10);
        testHarness.setProcessingTime(5011);

        ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();
        expected.add(new StreamRecord<>(testEnrichedTrxResult));

        ConcurrentLinkedQueue<Object> actual = testHarness.getOutput();

        TestHarnessUtil.assertOutputEquals("Output not as expected.", expected, actual);
    }
}
