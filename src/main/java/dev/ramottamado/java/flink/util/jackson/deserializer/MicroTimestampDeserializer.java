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

package dev.ramottamado.java.flink.util.jackson.deserializer;

import java.time.Instant;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link MicroTimestampDeserializer} allows for deserializing Debezium {@code MicroTimestamp} into
 * {@link java.time.Instant}.
 */
public class MicroTimestampDeserializer extends StdDeserializer<Instant> {
    private static final long serialVersionUID = 8178417124781L;
    private static final Logger logger = LoggerFactory.getLogger(MicroTimestampDeserializer.class);

    /**
     * The {@link MicroTimestampDeserializer} allows for deserializing Debezium {@code MicroTimestamp} into
     * {@link java.time.Instant}.
     */
    public MicroTimestampDeserializer() {
        super(Instant.class);
    }

    /**
     * The {@link MicroTimestampDeserializer} allows for deserializing Debezium {@code MicroTimestamp} into
     * {@link java.time.Instant}.
     *
     * @param vc the value class of serialized data
     */
    public MicroTimestampDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public Instant deserialize(JsonParser parser, DeserializationContext ctx) {
        try {
            long timestamp = parser.getLongValue() / 1000000;

            return Instant.ofEpochSecond(timestamp, 0);
        } catch (Exception e) {
            logger.error("Local date is not valid.", e);

            return null;
        }
    }
}
