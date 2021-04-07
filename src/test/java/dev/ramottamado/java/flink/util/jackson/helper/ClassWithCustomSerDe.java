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

package dev.ramottamado.java.flink.util.jackson.helper;

import java.time.Instant;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import dev.ramottamado.java.flink.util.jackson.deserializer.MicroTimestampDeserializer;
import dev.ramottamado.java.flink.util.jackson.serializer.TimestampSerializer;

public class ClassWithCustomSerDe {
    @JsonSerialize(using = TimestampSerializer.class)
    @JsonDeserialize(using = MicroTimestampDeserializer.class)
    @JsonProperty("timestamp")
    private Instant timestamp;

    @JsonProperty("another_timestamp")
    private Instant anotherTimestamp;

    public ClassWithCustomSerDe() {
    }

    @JsonProperty("timestamp")
    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    @JsonProperty("timestamp")
    public Instant getTimestamp() {
        return timestamp;
    }

    @JsonProperty("another_timestamp")
    public void setAnotherTimestamp(Instant anotherTimestamp) {
        this.anotherTimestamp = anotherTimestamp;
    }

    @JsonProperty("another_timestamp")
    public Instant getAnotherTimestamp() {
        return anotherTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && getClass() == o.getClass()) {
            ClassWithCustomSerDe that = (ClassWithCustomSerDe) o;

            return ((this.timestamp == null ? that.getTimestamp() == null : this.timestamp.equals(that.getTimestamp()))
                    && (this.anotherTimestamp == null ? that.getAnotherTimestamp() == null
                            : this.anotherTimestamp.equals(that.getAnotherTimestamp())));
        }

        return false;
    }
}
