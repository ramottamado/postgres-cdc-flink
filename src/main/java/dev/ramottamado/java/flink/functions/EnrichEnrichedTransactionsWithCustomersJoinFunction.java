package dev.ramottamado.java.flink.functions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.ramottamado.java.flink.schema.Customers;
import dev.ramottamado.java.flink.schema.EnrichedTransactions;

public class EnrichEnrichedTransactionsWithCustomersJoinFunction
        extends KeyedCoProcessFunction<String, EnrichedTransactions, Customers, EnrichedTransactions> {

    private ValueState<Customers> referenceDataState;

    private static final long serialVersionUID = 12319238113L;

    private static final Logger logger = LoggerFactory
            .getLogger(EnrichEnrichedTransactionsWithCustomersJoinFunction.class);

    @Override
    public void open(Configuration parameters) {

        ValueStateDescriptor<Customers> descriptor = new ValueStateDescriptor<>(
                "customers",
                TypeInformation.of(Customers.class)
        );

        referenceDataState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement1(EnrichedTransactions value, Context ctx, Collector<EnrichedTransactions> out)
            throws Exception {

        Customers customersState = referenceDataState.value();

        if (ctx.getCurrentKey() != "NULL" && customersState != null) {
            value.setDestName(customersState.getFirstName() + " " + customersState.getLastName());
            out.collect(value);
        } else if (customersState == null) {
            out.collect(value); // FIXME: use onTimer.
        } else {
            out.collect(value);
        }
    }

    @Override
    public void processElement2(Customers value, Context ctx, Collector<EnrichedTransactions> collector)
            throws Exception {

        referenceDataState.update(value);
    }
}
