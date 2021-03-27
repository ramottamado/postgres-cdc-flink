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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;

import dev.ramottamado.java.flink.KafkaTransactionsEnrichmentStreamingJob;
import dev.ramottamado.java.flink.schema.EnrichedTransactionsBean;

/**
 * The {@link EnrichedTransactionsToStringMapFunction} implements {@link MapFunction}
 * allows for serializing {@link EnrichedTransactionsBean} into JSON with pretty format.
 * Useful for printing {@link EnrichedTransactionsBean} record to STDOUT.
 *
 * @see KafkaTransactionsEnrichmentStreamingJob#writeEnrichedTransactionsOutput(DataStream, ParameterTool)
 */
public class EnrichedTransactionsToStringMapFunction implements MapFunction<EnrichedTransactionsBean, String> {
    private final static long serialVersionUID = -129393123132L;
    private ObjectMapper mapper;

    @Override
    public String map(EnrichedTransactionsBean value) {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }

        return mapper.valueToTree(value).toPrettyString();
    }
}
