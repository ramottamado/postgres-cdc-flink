/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.ramottamado.java.flink;

import java.text.MessageFormat;
import java.util.Properties;
import java.util.UUID;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

// import dev.ramottamado.java.flink.serialization.JSONValueDeserializationSchema;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the
 * <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>
 * To package your application into a JAR file for execution, run 'mvn clean
 * package' on the command line.
 *
 * <p>
 * If you change the name of the main class (with the public static void
 * main(String[] args)) method, change the respective entry in the POM.xml file
 * (simply search for 'mainClass').
 */
public class PostgreCdcStreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("group.id", MessageFormat.format("flink_consumer_{0}", UUID.randomUUID()));
		props.setProperty("auto.offset.reset", "earliest");

		DataStream<ObjectNode> postgreCdcStream = env.addSource(
				new FlinkKafkaConsumer<>("postgres.public.users", new JSONKeyValueDeserializationSchema(false), props));

		DataStream<String> parsedStream = postgreCdcStream.map(record -> {
			try {
				return record.get("value").get("payload").get("after").get("name").asText();
			} catch (Exception e) {
				e.printStackTrace();
				return "";
			}
		});

		parsedStream.print();

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
