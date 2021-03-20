package dev.ramottamado.java.flink.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import dev.ramottamado.java.flink.schema.EnrichedTransactions;

public class EnrichedTransactionsToStringMapFunction implements MapFunction<EnrichedTransactions, String> {

    private final static long serialVersionUID = -129393123132L;

    private ObjectMapper mapper;

    @Override
    public String map(EnrichedTransactions value) throws Exception {

        if (mapper == null) {
            mapper = new ObjectMapper();
        }

        return mapper.valueToTree(value).toPrettyString();
    }
}
