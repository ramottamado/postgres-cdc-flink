package dev.ramottamado.java.flink.functions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class PostgresJoinFunction extends KeyedCoProcessFunction<String, ObjectNode, ObjectNode, ObjectNode> {

    private static final long serialVersionUID = 12319238113L;

    private ValueState<ObjectNode> referenceDataState = null;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<ObjectNode> descriptor = new ValueStateDescriptor<>("state",
                TypeInformation.of(ObjectNode.class));

        referenceDataState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement1(ObjectNode value, Context ctx, Collector<ObjectNode> collector) throws Exception {
    }

    @Override
    public void processElement2(ObjectNode value, Context ctx, Collector<ObjectNode> collector) throws Exception {
        referenceDataState.update(value);
    }

}
