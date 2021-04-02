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

import java.time.Instant;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import dev.ramottamado.java.flink.util.jackson.helper.ClassWithCustomSerDe;

public class TimestampSerializerTest {
    private Instant timestamp = Instant.parse("2021-03-21T19:00:07.00Z");
    private String dummyJson = "{\"timestamp\":\"2021-03-21 19:00:07\"}";
    private ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testSerialize() throws Exception {
        ClassWithCustomSerDe testClass = new ClassWithCustomSerDe();
        testClass.setTimestamp(timestamp);

        String out = mapper.writeValueAsString(testClass);
        Assert.assertNotNull(out);
        Assert.assertEquals(dummyJson, out);
    }
}
