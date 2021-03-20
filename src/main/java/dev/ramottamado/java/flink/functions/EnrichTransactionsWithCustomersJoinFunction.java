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

public class EnrichTransactionsWithCustomersJoinFunction
        extends KeyedCoProcessFunction<String, Transactions, Customers, EnrichedTransactions> {

    private static final long serialVersionUID = 12319238113L;
    private ValueState<Customers> referenceDataState;
    private ValueState<TrxWithTimestamp> latestTrx;

    private class TrxWithTimestamp {

        long timestamp;
        Transactions trx;
    }

    @Override
    public void open(Configuration parameters) {

        ValueStateDescriptor<Customers> cDescriptor = new ValueStateDescriptor<>(
                "customers",
                TypeInformation.of(Customers.class)
        );

        ValueStateDescriptor<TrxWithTimestamp> trxWTimestampDescriptor = new ValueStateDescriptor<>(
                "trxWithTimestamp",
                TypeInformation.of(TrxWithTimestamp.class)
        );

        referenceDataState = getRuntimeContext().getState(cDescriptor);
        latestTrx = getRuntimeContext().getState(trxWTimestampDescriptor);
    }

    @Override
    public void processElement1(Transactions value, Context ctx, Collector<EnrichedTransactions> out) throws Exception {

        Customers customers = referenceDataState.value();

        if (customers != null) {
            out.collect(joinTrxWithCustomers(value, customers));
        } else {
            TrxWithTimestamp trxWithTimestamp = new TrxWithTimestamp();

            trxWithTimestamp.timestamp = ctx.timestamp();
            trxWithTimestamp.trx = value;
            latestTrx.update(trxWithTimestamp);
            ctx.timerService().registerProcessingTimeTimer(trxWithTimestamp.timestamp + 5000L);
        }
    }

    @Override
    public void processElement2(Customers value, Context ctx, Collector<EnrichedTransactions> out) throws Exception {

        referenceDataState.update(value);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<EnrichedTransactions> out) throws Exception {

        TrxWithTimestamp lastTrx = latestTrx.value();
        EnrichedTransactions etx = new EnrichedTransactions();

        if (referenceDataState.value() != null) {
            etx = joinTrxWithCustomers(lastTrx.trx, referenceDataState.value());
        } else {
            etx.setAmount(lastTrx.trx.getAmount());
            etx.setSrcAccount(lastTrx.trx.getSrcAccount());
            etx.setDestAcct(lastTrx.trx.getDestAcct());
            etx.setTrxTimestamp(lastTrx.trx.getTrxTimestamp());
            etx.setTrxType(lastTrx.trx.getTrxType());
        }

        latestTrx.clear();
        out.collect(etx);
    }

    public EnrichedTransactions joinTrxWithCustomers(Transactions trx, Customers cust) {

        EnrichedTransactions enrichedTrx = new EnrichedTransactions();

        enrichedTrx.setCif(cust.getCif());
        enrichedTrx.setAmount(trx.getAmount());
        enrichedTrx.setSrcAccount(trx.getSrcAccount());
        enrichedTrx.setDestAcct(trx.getDestAcct());
        enrichedTrx.setTrxTimestamp(trx.getTrxTimestamp());
        enrichedTrx.setTrxType(trx.getTrxType());
        enrichedTrx.setSrcName(cust.getFirstName() + " " + cust.getLastName());

        return enrichedTrx;
    }
}
