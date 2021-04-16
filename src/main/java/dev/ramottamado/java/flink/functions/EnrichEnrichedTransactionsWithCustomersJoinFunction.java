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

import java.util.Objects;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import dev.ramottamado.java.flink.schema.Customer;
import dev.ramottamado.java.flink.schema.EnrichedTransaction;
import dev.ramottamado.java.flink.schema.EnrichedTransactionWithTimestamp;

/**
 * The {@link EnrichEnrichedTransactionsWithCustomersJoinFunction} implements {@link KeyedCoProcessFunction}
 * to join the {@link EnrichedTransaction} stream with {@link Customer} stream.
 *
 * @author Tamado Sitohang
 * @since  1.0
 */
public class EnrichEnrichedTransactionsWithCustomersJoinFunction
        extends KeyedCoProcessFunction<String, EnrichedTransaction, Customer, EnrichedTransaction> {
    private static final long serialVersionUID = 12319238113L;
    private ValueState<Customer> referenceDataState;
    private ValueState<EnrichedTransactionWithTimestamp> latestEnrichedTrx;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Customer> cDescriptor = new ValueStateDescriptor<>(
                "customers",
                TypeInformation.of(Customer.class));

        ValueStateDescriptor<EnrichedTransactionWithTimestamp> eDescriptor = new ValueStateDescriptor<>(
                "enrichedTransactions",
                TypeInformation.of(EnrichedTransactionWithTimestamp.class));

        referenceDataState = getRuntimeContext().getState(cDescriptor);
        latestEnrichedTrx = getRuntimeContext().getState(eDescriptor);
    }

    @Override
    public void processElement1(EnrichedTransaction value, Context ctx, Collector<EnrichedTransaction> out)
            throws Exception {
        Customer customersState = referenceDataState.value();

        if (Objects.equals(ctx.getCurrentKey(), "NULL")) {
            out.collect(value);
        } else if (customersState != null) {
            value.setDestName(customersState.getFirstName() + " " + customersState.getLastName());
            out.collect(value);
        } else {
            EnrichedTransactionWithTimestamp etxWithTimestamp = new EnrichedTransactionWithTimestamp();
            etxWithTimestamp.setTimestamp(ctx.timestamp());
            etxWithTimestamp.setEtx(value);

            latestEnrichedTrx.update(etxWithTimestamp);
            ctx.timerService().registerProcessingTimeTimer(etxWithTimestamp.getTimestamp() + 5000L);
        }
    }

    @Override
    public void processElement2(Customer value, Context ctx, Collector<EnrichedTransaction> collector)
            throws Exception {
        referenceDataState.update(value);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<EnrichedTransaction> out) throws Exception {
        EnrichedTransactionWithTimestamp lastEtx = latestEnrichedTrx.value();
        Customer cust = referenceDataState.value();
        EnrichedTransaction etx = lastEtx.getEtx();

        if (cust != null) {
            etx.setDestName((cust.getFirstName() + " " + cust.getLastName()).trim());
        }

        latestEnrichedTrx.clear();
        out.collect(etx);
    }
}
