package dev.ramottamado.java.flink.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import dev.ramottamado.java.flink.schema.EnrichedTransactionsBean;

/**
 * The {@link EnrichedTransactionsToStringMapFunction} implements
 * {@link org.apache.flink.api.common.functions.MapFunction}
 * allows for serializing {@link EnrichedTransactionsBean} into JSON with pretty format.
 * Useful for printing {@link EnrichedTransactionsBean} record to STDOUT.
 *
 * @see dev.ramottamado.java.flink.KafkaTransactionsEnrichmentStreamingJob#writeEnrichedTransactionsOutput(
 *      org.apache.flink.streaming.api.datastream.DataStream,
 *      org.apache.flink.api.java.utils.ParameterTool)
 */
public class EnrichedTransactionsToStringMapFunction implements MapFunction<EnrichedTransactionsBean, String> {
    private final static long serialVersionUID = -129393123132L;
    private ObjectMapper mapper;

    @Override
    public String map(EnrichedTransactionsBean value) throws Exception {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }

        return mapper.valueToTree(value).toPrettyString();
    }
}
