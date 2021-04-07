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

package dev.ramottamado.java.flink.util.jackson.serializer;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

/**
 * The {@link TimestampSerializer} allows for serializing {@link java.time.Instant}
 * into parsed timestamp without zone info.
 */
public class TimestampSerializer extends StdSerializer<Instant> {
    private static final long serialVersionUID = 123718191L;
    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("Z"));

    /**
     * The {@link TimestampSerializer} allows for serializing {@link java.time.Instant}
     * into parsed timestamp without zone info.
     */
    public TimestampSerializer() {
        super(Instant.class);
    }

    /**
     * The {@link TimestampSerializer} allows for serializing {@link java.time.Instant}
     * into parsed timestamp without zone info.
     *
     * @param type the type of deserialized POJO ({@link java.time.Instant})
     */
    public TimestampSerializer(Class<Instant> type) {
        super(type);
    }

    @Override
    public void serialize(Instant value, JsonGenerator jg, SerializerProvider sp)
            throws IOException, DateTimeException {
        String parsedTimestamp = formatter.format(value);

        jg.writeString(parsedTimestamp);
    }
}
