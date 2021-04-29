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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The {@link JSONValueDeserializationSchema} describes how to deserialize message from Debezium
 * into {@link ObjectNode}.
 *
 * @author     Tamado Sitohang
 * @since      1.0
 * @deprecated Use custom serialization with {@link DebeziumJSONEnvelopeDeserializationSchema} as {@link ObjectNode} is
 *             not POJO
 */
@Deprecated
public class JSONValueDeserializationSchema extends AbstractDeserializationSchema<ObjectNode> {
    private static final long serialVersionUID = -91238719810201L;
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public ObjectNode deserialize(final byte[] message) throws IOException {
        final ObjectNode node = mapper.createObjectNode();

        if (message != null) {
            node.set("value", mapper.readValue(message, JsonNode.class));
        }

        return node;
    }

    @Override
    public boolean isEndOfStream(final ObjectNode nextElement) {
        return false; // Unbounded stream
    }

    @Override
    public TypeInformation<ObjectNode> getProducedType() {
        return TypeExtractor.getForClass(ObjectNode.class);
    }
}
