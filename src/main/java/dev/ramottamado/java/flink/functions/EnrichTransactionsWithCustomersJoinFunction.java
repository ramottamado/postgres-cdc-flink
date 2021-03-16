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

    private ValueState<Customers> referenceDataState = null;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Customers> descriptor = new ValueStateDescriptor<>("state",
                TypeInformation.of(Customers.class));

        referenceDataState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement1(Transactions value, Context ctx, Collector<EnrichedTransactions> collector)
            throws Exception {
        Customers customersState = referenceDataState.value();
        if (customersState != null) {
            EnrichedTransactions enrichedTrx = joinTrxWithCustomers(value, customersState);
            collector.collect(enrichedTrx);
        } else {
            EnrichedTransactions etx = new EnrichedTransactions();
            etx.setAmount(value.getAmount());
            etx.setSrcAccount(value.getSrcAccount());
            etx.setDestAcct(value.getDestAcct());
            etx.setTrxTimestamp(value.getTrxTimestamp());
            etx.setTrxType(value.getTrxType());

            collector.collect(etx);
        }
    }

    @Override
    public void processElement2(Customers value, Context ctx, Collector<EnrichedTransactions> collector)
            throws Exception {
        referenceDataState.update(value);
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
