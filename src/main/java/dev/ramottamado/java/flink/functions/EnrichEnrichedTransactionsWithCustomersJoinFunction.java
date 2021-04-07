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

import dev.ramottamado.java.flink.schema.CustomersBean;
import dev.ramottamado.java.flink.schema.EnrichedTransactionsBean;
import dev.ramottamado.java.flink.schema.EnrichedTransactionsWithTimestampBean;

/**
 * The {@link EnrichEnrichedTransactionsWithCustomersJoinFunction} implements {@link KeyedCoProcessFunction}
 * to join the {@link EnrichedTransactionsBean} stream with {@link CustomersBean} stream.
 */
public class EnrichEnrichedTransactionsWithCustomersJoinFunction
        extends KeyedCoProcessFunction<String, EnrichedTransactionsBean, CustomersBean, EnrichedTransactionsBean> {
    private static final long serialVersionUID = 12319238113L;
    private ValueState<CustomersBean> referenceDataState;
    private ValueState<EnrichedTransactionsWithTimestampBean> latestEnrichedTrx;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<CustomersBean> cDescriptor = new ValueStateDescriptor<>(
                "customers",
                TypeInformation.of(CustomersBean.class));

        ValueStateDescriptor<EnrichedTransactionsWithTimestampBean> eDescriptor = new ValueStateDescriptor<>(
                "enrichedTransactions",
                TypeInformation.of(EnrichedTransactionsWithTimestampBean.class));

        referenceDataState = getRuntimeContext().getState(cDescriptor);
        latestEnrichedTrx = getRuntimeContext().getState(eDescriptor);
    }

    @Override
    public void processElement1(EnrichedTransactionsBean value, Context ctx, Collector<EnrichedTransactionsBean> out)
            throws Exception {
        CustomersBean customersState = referenceDataState.value();

        if (Objects.equals(ctx.getCurrentKey(), "NULL")) {
            out.collect(value);
        } else if (customersState != null) {
            value.setDestName(customersState.getFirstName() + " " + customersState.getLastName());
            out.collect(value);
        } else {
            EnrichedTransactionsWithTimestampBean etxWithTimestamp = new EnrichedTransactionsWithTimestampBean();
            etxWithTimestamp.setTimestamp(ctx.timestamp());
            etxWithTimestamp.setEtx(value);

            latestEnrichedTrx.update(etxWithTimestamp);
            ctx.timerService().registerProcessingTimeTimer(etxWithTimestamp.getTimestamp() + 5000L);
        }
    }

    @Override
    public void processElement2(CustomersBean value, Context ctx, Collector<EnrichedTransactionsBean> collector)
            throws Exception {
        referenceDataState.update(value);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<EnrichedTransactionsBean> out) throws Exception {
        EnrichedTransactionsWithTimestampBean lastEtx = latestEnrichedTrx.value();
        CustomersBean cust = referenceDataState.value();
        EnrichedTransactionsBean etx = lastEtx.getEtx();

        if (cust != null) {
            etx.setDestName((cust.getFirstName() + " " + cust.getLastName()).trim());
        }

        latestEnrichedTrx.clear();
        out.collect(etx);
    }
}
