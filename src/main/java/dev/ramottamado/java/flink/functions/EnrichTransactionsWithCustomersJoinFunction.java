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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import dev.ramottamado.java.flink.schema.CustomersBean;
import dev.ramottamado.java.flink.schema.EnrichedTransactionsBean;
import dev.ramottamado.java.flink.schema.TransactionsBean;
import dev.ramottamado.java.flink.schema.TransactionsWithTimestampBean;

/**
 * The {@link EnrichTransactionsWithCustomersJoinFunction} implements {@link KeyedCoProcessFunction}
 * to join the {@link TransactionsBean} stream with {@link CustomersBean} stream.
 */
public class EnrichTransactionsWithCustomersJoinFunction
        extends KeyedCoProcessFunction<String, TransactionsBean, CustomersBean, EnrichedTransactionsBean> {
    private static final long serialVersionUID = 12319238113L;
    private ValueState<CustomersBean> referenceDataState;
    private ValueState<TransactionsWithTimestampBean> latestTrx;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<CustomersBean> cDescriptor = new ValueStateDescriptor<>(
                "customers",
                TypeInformation.of(CustomersBean.class));

        ValueStateDescriptor<TransactionsWithTimestampBean> tDescriptor = new ValueStateDescriptor<>(
                "trxWithTimestamp",
                TypeInformation.of(TransactionsWithTimestampBean.class));

        referenceDataState = getRuntimeContext().getState(cDescriptor);
        latestTrx = getRuntimeContext().getState(tDescriptor);
    }

    @Override
    public void processElement1(TransactionsBean value, Context ctx, Collector<EnrichedTransactionsBean> out)
            throws Exception {
        CustomersBean customers = referenceDataState.value();

        if (customers != null) {
            out.collect(joinTrxWithCustomers(value, customers));
        } else {
            TransactionsWithTimestampBean trxWithTimestamp = new TransactionsWithTimestampBean();
            trxWithTimestamp.setTimestamp(ctx.timestamp());
            trxWithTimestamp.setTrx(value);

            latestTrx.update(trxWithTimestamp);
            ctx.timerService().registerProcessingTimeTimer(trxWithTimestamp.getTimestamp() + 5000L);
        }
    }

    @Override
    public void processElement2(CustomersBean value, Context ctx, Collector<EnrichedTransactionsBean> out)
            throws Exception {
        referenceDataState.update(value);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<EnrichedTransactionsBean> out) throws Exception {
        TransactionsWithTimestampBean lastTrx = latestTrx.value();
        EnrichedTransactionsBean enrichedTrx = new EnrichedTransactionsBean();

        if (referenceDataState.value() != null) {
            enrichedTrx = joinTrxWithCustomers(lastTrx.getTrx(), referenceDataState.value());
        } else {
            enrichedTrx.setAmount(lastTrx.getTrx().getAmount());
            enrichedTrx.setSrcAcct(lastTrx.getTrx().getSrcAcct());
            enrichedTrx.setDestAcct(lastTrx.getTrx().getDestAcct());
            enrichedTrx.setTrxTimestamp(lastTrx.getTrx().getTrxTimestamp());
            enrichedTrx.setTrxType(lastTrx.getTrx().getTrxType());
        }

        latestTrx.clear();
        out.collect(enrichedTrx);
    }

    /**
     * Enrich {@link TransactionsBean} with {@link CustomersBean}, returning new {@link EnrichedTransactionsBean}.
     *
     * @param  trx  the transaction to enrich
     * @param  cust the customer used to enrich the transaction record
     * @return      the enriched transaction
     */
    private EnrichedTransactionsBean joinTrxWithCustomers(TransactionsBean trx, CustomersBean cust) {
        EnrichedTransactionsBean enrichedTrx = new EnrichedTransactionsBean();

        enrichedTrx.setCif(cust.getCif());
        enrichedTrx.setAmount(trx.getAmount());
        enrichedTrx.setSrcAcct(trx.getSrcAcct());
        enrichedTrx.setDestAcct(trx.getDestAcct());
        enrichedTrx.setTrxTimestamp(trx.getTrxTimestamp());
        enrichedTrx.setTrxType(trx.getTrxType());
        enrichedTrx.setSrcName((cust.getFirstName() + " " + cust.getLastName()).trim());

        return enrichedTrx;
    }
}
