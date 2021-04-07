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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import dev.ramottamado.java.flink.util.jackson.helper.ClassWithCustomSerDe;

public class TimestampSerializerTest {
    private final Instant timestamp = Instant.parse("2021-03-21T19:00:07.00Z");
    private final ObjectMapper mapper = new ObjectMapper();
    private final String dummyJson = "{\"timestamp\":\"2021-03-21 19:00:07\",\"another_timestamp\":\"2021-03-21 19:00:07\"}";
    private ClassWithCustomSerDe testClass;

    @Before
    public void prepareTest() {
        JsonSerializer<Instant> cusSerializer = new TimestampSerializer(Instant.class);

        SimpleModule module = new SimpleModule();
        module.addSerializer(Instant.class, cusSerializer);
        mapper.registerModule(module);

        testClass = new ClassWithCustomSerDe();
        testClass.setTimestamp(timestamp);
        testClass.setAnotherTimestamp(timestamp);
    }

    @Test
    public void testSerialize() throws Exception {
        String actual = mapper.writeValueAsString(testClass);

        Assert.assertNotNull(actual);
        Assert.assertEquals(dummyJson, actual);
    }

    @Test
    public void testSerializeWithEmptyConstructor() throws Exception {
        ObjectMapper newMapper = new ObjectMapper();
        JsonSerializer<Instant> cusSerializer = new TimestampSerializer();

        SimpleModule newModule = new SimpleModule();
        newModule.addSerializer(Instant.class, cusSerializer);
        newMapper.registerModule(newModule);

        String actual = newMapper.writeValueAsString(testClass);

        Assert.assertNotNull(actual);
        Assert.assertEquals(dummyJson, actual);
    }
}
