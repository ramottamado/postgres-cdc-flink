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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;

import dev.ramottamado.java.flink.KafkaTransactionsEnrichmentStreamingJob;

/**
 * The {@link EnrichedTransactionsToStringMapFunction} implements {@link MapFunction} allows for deserializing Debezium
 * JSON envelope into POJO.
 *
 * @param <T> the type of deserialized POJO
 * @see       KafkaTransactionsEnrichmentStreamingJob#writeEnrichedTransactionsOutput(DataStream,ParameterTool)
 */
public class EnvelopeParserMapFunction<T> implements MapFunction<ObjectNode, T> {
    private static final long serialVersionUID = 123456672L;
    private final Class<T> type;
    private ObjectMapper mapper;

    /**
     * The {@link EnrichedTransactionsToStringMapFunction} implements
     * {@link org.apache.flink.api.common.functions.MapFunction}
     * allows for deserializing Debezium JSON envelope into POJO.
     *
     * @param type the type of deserialized POJO
     */
    public EnvelopeParserMapFunction(Class<T> type) {
        this.type = type;
    }

    @Override
    public T map(ObjectNode value) throws Exception {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }

        try {
            return mapper.treeToValue(value.get("value").get("payload").get("after"), type);
        } catch (Exception e) {
            return type.newInstance();
        }
    }
}
