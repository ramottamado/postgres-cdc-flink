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

import dev.ramottamado.java.flink.schema.Customers;
import dev.ramottamado.java.flink.schema.EnrichedTransactions;
import dev.ramottamado.java.flink.schema.Transactions;
import dev.ramottamado.java.flink.schema.TransactionsWithTimestamp;

/**
 * The {@link EnrichTransactionsWithCustomersJoinFunction} implements {@link KeyedCoProcessFunction}
 * to join the {@link Transactions} stream with {@link Customers} stream.
 *
 * @author Tamado Sitohang
 * @since  1.0
 */
public class EnrichTransactionsWithCustomersJoinFunction
        extends KeyedCoProcessFunction<String, Transactions, Customers, EnrichedTransactions> {
    private static final long serialVersionUID = 12319238113L;
    private ValueState<Customers> referenceDataState;
    private ValueState<TransactionsWithTimestamp> latestTrx;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Customers> cDescriptor = new ValueStateDescriptor<>(
                "customers",
                TypeInformation.of(Customers.class));

        ValueStateDescriptor<TransactionsWithTimestamp> tDescriptor = new ValueStateDescriptor<>(
                "trxWithTimestamp",
                TypeInformation.of(TransactionsWithTimestamp.class));

        referenceDataState = getRuntimeContext().getState(cDescriptor);
        latestTrx = getRuntimeContext().getState(tDescriptor);
    }

    @Override
    public void processElement1(Transactions value, Context ctx, Collector<EnrichedTransactions> out)
            throws Exception {
        Customers customers = referenceDataState.value();

        if (customers != null) {
            out.collect(joinTrxWithCustomers(value, customers));
        } else {
            TransactionsWithTimestamp trxWithTimestamp = new TransactionsWithTimestamp();
            trxWithTimestamp.setTimestamp(ctx.timestamp());
            trxWithTimestamp.setTrx(value);

            latestTrx.update(trxWithTimestamp);
            ctx.timerService().registerProcessingTimeTimer(trxWithTimestamp.getTimestamp() + 5000L);
        }
    }

    @Override
    public void processElement2(Customers value, Context ctx, Collector<EnrichedTransactions> out)
            throws Exception {
        referenceDataState.update(value);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<EnrichedTransactions> out) throws Exception {
        TransactionsWithTimestamp lastTrx = latestTrx.value();
        EnrichedTransactions enrichedTrx = new EnrichedTransactions();

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
     * Enrich {@link Transactions} with {@link Customers}, returning new {@link EnrichedTransactions}.
     *
     * @param  trx  the transaction to enrich
     * @param  cust the customer used to enrich the transaction record
     * @return      the enriched transaction
     */
    private EnrichedTransactions joinTrxWithCustomers(Transactions trx, Customers cust) {
        EnrichedTransactions enrichedTrx = new EnrichedTransactions();

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
