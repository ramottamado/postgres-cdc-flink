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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import dev.ramottamado.java.flink.util.jackson.helper.ClassWithCustomSerDe;

public class MicroTimestampDeserializerTest {
    private final Instant timestamp = Instant.parse("2021-03-21T19:00:07.00Z");
    private final ObjectMapper mapper = new ObjectMapper();
    private String dummyJson = "{\"timestamp\":1616353207000000,\"another_timestamp\":1616353207000000}";
    private ClassWithCustomSerDe expected;

    @Before
    public void prepareTest() {
        JsonDeserializer<Instant> cusDeserializer = new MicroTimestampDeserializer(Instant.class);

        SimpleModule module = new SimpleModule();
        module.addDeserializer(Instant.class, cusDeserializer);
        mapper.registerModule(module);

        expected = new ClassWithCustomSerDe();
        expected.setTimestamp(timestamp);
        expected.setAnotherTimestamp(timestamp);
    }

    @Test
    public void testDeserialize() throws Exception {
        ClassWithCustomSerDe actual = mapper.readValue(dummyJson, ClassWithCustomSerDe.class);

        Assert.assertNotNull(actual);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testDeserializeWithEmptyConstructor() throws Exception {
        JsonDeserializer<Instant> cusDeserializer = new MicroTimestampDeserializer();
        ObjectMapper newMapper = new ObjectMapper();

        SimpleModule newModule = new SimpleModule();
        newModule.addDeserializer(Instant.class, cusDeserializer);
        newMapper.registerModule(newModule);

        ClassWithCustomSerDe actual = newMapper.readValue(dummyJson, ClassWithCustomSerDe.class);

        Assert.assertNotNull(actual);
        Assert.assertEquals(expected,actual);
    }

    @Test
    public void testDeserializeWithError() throws Exception {
        dummyJson =
                "{\"timestamp\":1616353207000000000000000000000000000000000,\"another_timestamp\":1616353207000000000000000000000000000000000}";

        ClassWithCustomSerDe actual = mapper.readValue(dummyJson, ClassWithCustomSerDe.class);

        Assert.assertNotNull(actual);
        Assert.assertNull(actual.getTimestamp());
        Assert.assertNull(actual.getAnotherTimestamp());
    }
}
