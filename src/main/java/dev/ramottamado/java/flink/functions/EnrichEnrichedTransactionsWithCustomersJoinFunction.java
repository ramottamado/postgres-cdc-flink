package dev.ramottamado.java.flink.functions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import dev.ramottamado.java.flink.schema.Customers;
import dev.ramottamado.java.flink.schema.EnrichedTransactions;

public class EnrichEnrichedTransactionsWithCustomersJoinFunction
        extends KeyedCoProcessFunction<String, EnrichedTransactions, Customers, EnrichedTransactions> {

    private static final long serialVersionUID = 12319238113L;

    private ValueState<Customers> referenceDataState = null;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Customers> descriptor = new ValueStateDescriptor<>("state2",
                TypeInformation.of(Customers.class));

        referenceDataState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement1(EnrichedTransactions value, Context ctx, Collector<EnrichedTransactions> collector)
            throws Exception {
        Customers customersState = referenceDataState.value();
        if (ctx.getCurrentKey() != "NULL") {
            if (customersState != null) {
                EnrichedTransactions enrichedTrx = joinTrxWithCustomers(value, customersState);
                collector.collect(enrichedTrx);
            } else {
                collector.collect(value);
            }
        } else {
            collector.collect(value);
        }
    }

    @Override
    public void processElement2(Customers value, Context ctx, Collector<EnrichedTransactions> collector)
            throws Exception {
        referenceDataState.update(value);
    }

    public EnrichedTransactions joinTrxWithCustomers(EnrichedTransactions trx, Customers cust) {
        trx.setDestName(cust.getFirstName() + " " + cust.getLastName());
        return trx;
    }

}
