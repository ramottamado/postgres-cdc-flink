package dev.ramottamado.java.flink.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import dev.ramottamado.java.flink.schema.Transactions;

public class TransactionsEnvelopeParserMapFunction implements MapFunction<ObjectNode, Transactions> {

    private static final long serialVersionUID = 123456672L;

    @Override
    public Transactions map(ObjectNode value) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        try {
            return mapper.treeToValue(value.get("value").get("payload").get("after"), Transactions.class);
        } catch (Exception e) {
            return new Transactions();
        }
    }

}
