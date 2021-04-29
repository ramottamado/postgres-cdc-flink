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

package dev.ramottamado.java.flink.util.serialization;

import java.io.IOException;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import dev.ramottamado.java.flink.annotation.PublicEvolving;

/**
 * The {@link DebeziumJSONEnvelopeDeserializationSchema} describes how to deserialize byte messages from Debezium
 * into POJOs.
 *
 * @param  <T> the type of POJO
 * @author     Tamado Sitohang
 * @since      1.0
 */
@PublicEvolving
public class DebeziumJSONEnvelopeDeserializationSchema<T> extends AbstractDeserializationSchema<T> {
    private static final long serialVersionUID = -91238719810201L;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Class<T> type;

    /**
     * The {@link DebeziumJSONEnvelopeDeserializationSchema} describes how to deserialize byte messages from Debezium
     * into POJOs.
     *
     * @param  type the type of POJO
     * @author      Tamado Sitohang
     * @since       1.0
     */
    public DebeziumJSONEnvelopeDeserializationSchema(final Class<T> type) {
        super(type);
        this.type = type;
    }

    @Override
    public T deserialize(final byte[] message) throws IOException {
        ObjectNode node = mapper.createObjectNode();

        if (message != null) {
            node.set("value", mapper.readValue(message, JsonNode.class));
        }

        try {
            return mapper.treeToValue(node.get("value").get("payload").get("after"), type);
        } catch (final Exception e) {
            node = node.removeAll();

            return mapper.treeToValue(node, type);
        }

    }
}
