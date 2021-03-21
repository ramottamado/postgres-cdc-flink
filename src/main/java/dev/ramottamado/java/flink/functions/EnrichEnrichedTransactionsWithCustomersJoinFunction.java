package dev.ramottamado.java.flink.functions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import dev.ramottamado.java.flink.schema.CustomersBean;
import dev.ramottamado.java.flink.schema.EnrichedTransactionsBean;

/**
 * The {@link EnrichEnrichedTransactionsWithCustomersJoinFunction} implements {@link KeyedCoProcessFunction}
 * to join the {@link EnrichedTransactionsBean} stream with {@link CustomersBean} stream.
 */
public class EnrichEnrichedTransactionsWithCustomersJoinFunction
        extends KeyedCoProcessFunction<String, EnrichedTransactionsBean, CustomersBean, EnrichedTransactionsBean> {
    private static final long serialVersionUID = 12319238113L;
    private ValueState<CustomersBean> referenceDataState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<CustomersBean> descriptor = new ValueStateDescriptor<>(
                "customers",
                TypeInformation.of(CustomersBean.class));

        referenceDataState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement1(EnrichedTransactionsBean value, Context ctx, Collector<EnrichedTransactionsBean> out)
            throws Exception {
        CustomersBean customersState = referenceDataState.value();

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
    public void processElement2(CustomersBean value, Context ctx, Collector<EnrichedTransactionsBean> collector)
            throws Exception {
        referenceDataState.update(value);
    }
}
