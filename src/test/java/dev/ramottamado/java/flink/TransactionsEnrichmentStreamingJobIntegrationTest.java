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
import dev.ramottamado.java.flink.schema.CustomersBean;
import dev.ramottamado.java.flink.schema.EnrichedTransactionsBean;
import dev.ramottamado.java.flink.schema.TransactionsBean;

public class TransactionsEnrichmentStreamingJobIntegrationTest {
    public CustomersBean cust1;
    public CustomersBean cust2;
    public TransactionsBean trx;
    public EnrichedTransactionsBean etx;

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    public static class CollectSink implements SinkFunction<EnrichedTransactionsBean> {
        public static final long serialVersionUID = 1328490872834124987L;
        public static final List<EnrichedTransactionsBean> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(EnrichedTransactionsBean value) {
            values.add(value);
        }
    }

    public static class TestTransactionsEnrichmentStreamingJob extends TransactionsEnrichmentStreamingJob {
        private final List<CustomersBean> customersBeans;
        private final List<TransactionsBean> transactionsBeans;

        public TestTransactionsEnrichmentStreamingJob(List<CustomersBean> customersBeans,
                List<TransactionsBean> transactionsBeans) {
            this.customersBeans = customersBeans;
            this.transactionsBeans = transactionsBeans;
        }

        @Override
        public StreamExecutionEnvironment createExecutionEnvironment() {
            return StreamExecutionEnvironment.getExecutionEnvironment();
        }

        @Override
        public DataStream<CustomersBean> readCustomersCdcStream() {
            return env.addSource(new TestSourceFunction<>(customersBeans, CustomersBean.class))
                    .assignTimestampsAndWatermarks(new TestTimestampAssigner<>());
        }

        @Override
        public DataStream<TransactionsBean> readTransactionsCdcStream() {
            return env
                    .addSource(new TestSourceFunction<>(transactionsBeans, TransactionsBean.class))
                    .assignTimestampsAndWatermarks(new TestTimestampAssigner<>());
        }

        @Override
        public void writeEnrichedTransactionsOutput(DataStream<EnrichedTransactionsBean> enrichedTrxStream) {
            enrichedTrxStream.addSink(new CollectSink());
        }
    }

    @Before
    public void prepareTest() {
        cust1 = new CustomersBean();
        cust2 = new CustomersBean();
        trx = new TransactionsBean();
        etx = new EnrichedTransactionsBean();

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

        List<CustomersBean> customersBeans = new ArrayList<>();
        customersBeans.add(cust1);
        customersBeans.add(cust2);

        List<TransactionsBean> transactionsBeans = new ArrayList<>();
        transactionsBeans.add(trx);

        StreamExecutionEnvironment env = new TestTransactionsEnrichmentStreamingJob(customersBeans, transactionsBeans)
                .createApplicationPipeline();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(2);
        env.execute();

        Assert.assertTrue(CollectSink.values.contains(etx));
    }
}
