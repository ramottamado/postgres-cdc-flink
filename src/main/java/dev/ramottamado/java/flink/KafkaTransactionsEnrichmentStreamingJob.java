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

package dev.ramottamado.java.flink;

import static dev.ramottamado.java.flink.config.ParameterConfig.CHECKPOINT_PATH;
import static dev.ramottamado.java.flink.config.ParameterConfig.DEBUG_RESULT_STREAM;
import static dev.ramottamado.java.flink.config.ParameterConfig.ENVIRONMENT;
import static dev.ramottamado.java.flink.config.ParameterConfig.KAFKA_OFFSET_STRATEGY;
import static dev.ramottamado.java.flink.config.ParameterConfig.KAFKA_SOURCE_TOPIC_1;
import static dev.ramottamado.java.flink.config.ParameterConfig.KAFKA_SOURCE_TOPIC_2;
import static dev.ramottamado.java.flink.config.ParameterConfig.KAFKA_TARGET_TOPIC;

import java.util.Objects;
import java.util.Properties;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import dev.ramottamado.java.flink.functions.EnrichedTransactionsToStringMapFunction;
import dev.ramottamado.java.flink.schema.CustomersBean;
import dev.ramottamado.java.flink.schema.EnrichedTransactionsBean;
import dev.ramottamado.java.flink.schema.TransactionsBean;
import dev.ramottamado.java.flink.util.ParameterUtils;
import dev.ramottamado.java.flink.util.kafka.KafkaProperties;
import dev.ramottamado.java.flink.util.serialization.DebeziumJSONEnvelopeDeserializationSchema;
import dev.ramottamado.java.flink.util.serialization.EnrichedTransactionsKafkaSerializationSchema;

/**
 * The class {@link KafkaTransactionsEnrichmentStreamingJob} provides {@link TransactionsEnrichmentStreamingJob}
 * for enriching {@link TransactionsBean} data using Flink.
 *
 * @see TransactionsEnrichmentStreamingJob
 */
public class KafkaTransactionsEnrichmentStreamingJob extends TransactionsEnrichmentStreamingJob {
    private final DebeziumJSONEnvelopeDeserializationSchema<TransactionsBean> tDeserializationSchema =
            new DebeziumJSONEnvelopeDeserializationSchema<>(TransactionsBean.class);

    private final DebeziumJSONEnvelopeDeserializationSchema<CustomersBean> cDeserializationSchema =
            new DebeziumJSONEnvelopeDeserializationSchema<>(CustomersBean.class);

    private final EnrichedTransactionsKafkaSerializationSchema etxSerializationSchema =
            new EnrichedTransactionsKafkaSerializationSchema("enriched_transactions");

    @Override
    public final DataStream<TransactionsBean> readTransactionsCdcStream() throws RuntimeException {
        Properties properties = KafkaProperties.getProperties(params);

        FlinkKafkaConsumer<TransactionsBean> tKafkaConsumer = new FlinkKafkaConsumer<>(
                params.getRequired(KAFKA_SOURCE_TOPIC_1),
                tDeserializationSchema,
                properties);

        String kafkaOffsetStrategy = params.get(KAFKA_OFFSET_STRATEGY, "inherit");

        if (Objects.equals(kafkaOffsetStrategy, "earliest")) {
            tKafkaConsumer.setStartFromEarliest();
        } else if (Objects.equals(kafkaOffsetStrategy, "latest")) {
            tKafkaConsumer.setStartFromLatest();
        } else {
            tKafkaConsumer.setStartFromGroupOffsets();
        }

        return env.addSource(tKafkaConsumer);
    }

    @Override
    public final DataStream<CustomersBean> readCustomersCdcStream()
            throws RuntimeException {
        Properties properties = KafkaProperties.getProperties(params);

        FlinkKafkaConsumer<CustomersBean> cKafkaConsumer =
                new FlinkKafkaConsumer<>(params.getRequired(KAFKA_SOURCE_TOPIC_2), cDeserializationSchema, properties);

        String kafkaOffsetStrategy = params.get(KAFKA_OFFSET_STRATEGY, "inherit");

        if (Objects.equals(kafkaOffsetStrategy, "earliest")) {
            cKafkaConsumer.setStartFromEarliest();
        } else if (Objects.equals(kafkaOffsetStrategy, "latest")) {
            cKafkaConsumer.setStartFromLatest();
        } else {
            cKafkaConsumer.setStartFromGroupOffsets();
        }

        return env.addSource(cKafkaConsumer);
    }

    @Override
    public final void writeEnrichedTransactionsOutput(DataStream<EnrichedTransactionsBean> enrichedTrxStream)
            throws RuntimeException {
        Properties properties = KafkaProperties.getProperties(params);

        FlinkKafkaProducer<EnrichedTransactionsBean> etxKafkaProducer = new FlinkKafkaProducer<>(
                params.get(KAFKA_TARGET_TOPIC, "enriched_transactions"),
                etxSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

        if (params.getBoolean(DEBUG_RESULT_STREAM, false))
            enrichedTrxStream.map(new EnrichedTransactionsToStringMapFunction()).print();

        enrichedTrxStream.addSink(etxKafkaProducer);
    }

    @Override
    public final StreamExecutionEnvironment createExecutionEnvironment() throws RuntimeException {
        String checkpointPath = params.getRequired(CHECKPOINT_PATH);
        StateBackend stateBackend = new FsStateBackend(checkpointPath);
        Configuration conf = new Configuration();

        conf.setString("state.backend", "filesystem");
        conf.setString("state.checkpoints.dir", checkpointPath);

        if (Objects.equals(params.get(ENVIRONMENT), "development")) {
            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setStateBackend(stateBackend);
        }

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        env.enableCheckpointing(10000L);

        return env;
    }

    /**
     * Main method to run the application.
     *
     * @param  args      the arguments to pass into the application
     * @throws Exception if some errors happened
     */
    public static void main(String[] args) throws Exception {
        params = ParameterUtils.parseArgs(args);

        new KafkaTransactionsEnrichmentStreamingJob()
                .createApplicationPipeline()
                .execute("Kafka Transactions Stream Enrichment");
    }
}
