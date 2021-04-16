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

package dev.ramottamado.java.flink;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import dev.ramottamado.java.flink.functions.helper.TestSourceFunction;
import dev.ramottamado.java.flink.functions.helper.TestTimestampAssigner;
import dev.ramottamado.java.flink.schema.Customer;
import dev.ramottamado.java.flink.schema.EnrichedTransaction;
import dev.ramottamado.java.flink.schema.Transaction;

public class TransactionsEnrichmentStreamingJobIntegrationTest {
    public Customer cust1;
    public Customer cust2;
    public Transaction trx;
    public EnrichedTransaction etx;

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    public static class CollectSink implements SinkFunction<EnrichedTransaction> {
        public static final long serialVersionUID = 1328490872834124987L;
        public static final List<EnrichedTransaction> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(EnrichedTransaction value) {
            values.add(value);
        }
    }

    public static class TestTransactionsEnrichmentStreamingJob extends AbstractTransactionsEnrichmentStreamingJob {
        private final List<Customer> customersBeans;
        private final List<Transaction> transactionsBeans;

        public TestTransactionsEnrichmentStreamingJob(List<Customer> customersBeans,
                List<Transaction> transactionsBeans) {
            this.customersBeans = customersBeans;
            this.transactionsBeans = transactionsBeans;
        }

        @Override
        public StreamExecutionEnvironment createExecutionEnvironment() {
            return StreamExecutionEnvironment.getExecutionEnvironment();
        }

        @Override
        public DataStream<Customer> readCustomersCdcStream() {
            return env.addSource(new TestSourceFunction<>(customersBeans, Customer.class))
                    .assignTimestampsAndWatermarks(new TestTimestampAssigner<>());
        }

        @Override
        public DataStream<Transaction> readTransactionsCdcStream() {
            return env
                    .addSource(new TestSourceFunction<>(transactionsBeans, Transaction.class))
                    .assignTimestampsAndWatermarks(new TestTimestampAssigner<>());
        }

        @Override
        public void writeEnrichedTransactionsOutput(DataStream<EnrichedTransaction> enrichedTrxStream) {
            enrichedTrxStream.addSink(new CollectSink());
        }
    }

    @Before
    public void prepareTest() {
        cust1 = new Customer();
        cust2 = new Customer();
        trx = new Transaction();
        etx = new EnrichedTransaction();

        cust1.setAcctNumber("0001");
        cust1.setCif("001");
        cust1.setCity("Bandung");
        cust1.setFirstName("Tamado");
        cust1.setLastName("Sitohang");

        cust2.setAcctNumber("0002");
        cust2.setCif("002");
        cust2.setCity("Jakarta");
        cust2.setFirstName("Kamal");
        cust2.setLastName("Rasyid");

        trx.setAmount(10000.0);
        trx.setDestAcct("0002");
        trx.setSrcAcct("0001");
        trx.setTrxTimestamp(Instant.parse("2021-01-01T12:00:00.00Z"));
        trx.setTrxType("TRANSFER");

        etx.setAmount(10000.0);
        etx.setDestAcct("0002");
        etx.setSrcAcct("0001");
        etx.setTrxTimestamp(Instant.parse("2021-01-01T12:00:00.00Z"));
        etx.setTrxType("TRANSFER");
        etx.setCif("001");
        etx.setSrcName("Tamado Sitohang");
        etx.setDestName("Kamal Rasyid");
    }

    @Test
    public void testCreateApplicationPipeline() throws Exception {
        CollectSink.values.clear();

        List<Customer> customersBeans = new ArrayList<>();
        customersBeans.add(cust1);
        customersBeans.add(cust2);

        List<Transaction> transactionsBeans = new ArrayList<>();
        transactionsBeans.add(trx);

        StreamExecutionEnvironment env = new TestTransactionsEnrichmentStreamingJob(customersBeans, transactionsBeans)
                .createApplicationPipeline();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(2);
        env.execute();

        Assert.assertTrue(CollectSink.values.contains(etx));
    }
}
