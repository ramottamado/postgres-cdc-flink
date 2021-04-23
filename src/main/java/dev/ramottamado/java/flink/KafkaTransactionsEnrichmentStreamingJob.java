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

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import dev.ramottamado.java.flink.annotation.Public;
import dev.ramottamado.java.flink.functions.EnrichedTransactionsToStringMapFunction;
import dev.ramottamado.java.flink.schema.Customer;
import dev.ramottamado.java.flink.schema.EnrichedTransaction;
import dev.ramottamado.java.flink.schema.Transaction;
import dev.ramottamado.java.flink.util.ParameterUtils;
import dev.ramottamado.java.flink.util.kafka.KafkaProperties;
import dev.ramottamado.java.flink.util.serialization.DebeziumJSONEnvelopeDeserializationSchema;
import dev.ramottamado.java.flink.util.serialization.EnrichedTransactionsKafkaSerializationSchema;

/**
 * The class {@code KafkaTransactionsEnrichmentStreamingJob} provides {@link AbstractTransactionsEnrichmentStreamingJob}
 * for enriching {@link Transaction} data using Flink.
 *
 * @author Tamado Sitohang
 * @see    AbstractTransactionsEnrichmentStreamingJob
 * @since  1.0
 */
@Public
public class KafkaTransactionsEnrichmentStreamingJob extends AbstractTransactionsEnrichmentStreamingJob {
    private final DebeziumJSONEnvelopeDeserializationSchema<Transaction> tDeserializationSchema =
            new DebeziumJSONEnvelopeDeserializationSchema<>(Transaction.class);

    private final DebeziumJSONEnvelopeDeserializationSchema<Customer> cDeserializationSchema =
            new DebeziumJSONEnvelopeDeserializationSchema<>(Customer.class);

    private final EnrichedTransactionsKafkaSerializationSchema etxSerializationSchema =
            new EnrichedTransactionsKafkaSerializationSchema("enriched_transactions");

    private void setFlinkKafkaConsumerOffsetStrategy(final ParameterTool params,
            final FlinkKafkaConsumer<?> kafkaConsumer) {
        final String kafkaOffsetStrategy = params.get(KAFKA_OFFSET_STRATEGY, "inherit");

        if (Objects.equals(kafkaOffsetStrategy, "earliest")) {
            kafkaConsumer.setStartFromEarliest();
        } else if (Objects.equals(kafkaOffsetStrategy, "latest")) {
            kafkaConsumer.setStartFromLatest();
        } else {
            kafkaConsumer.setStartFromGroupOffsets();
        }
    }

    @Override
    public final DataStream<Transaction> readTransactionsCdcStream() throws RuntimeException {
        final Properties properties = KafkaProperties.getProperties(params);

        final FlinkKafkaConsumer<Transaction> tKafkaConsumer = new FlinkKafkaConsumer<>(
                params.getRequired(KAFKA_SOURCE_TOPIC_1),
                tDeserializationSchema,
                properties);

        setFlinkKafkaConsumerOffsetStrategy(params, tKafkaConsumer);

        return env.addSource(tKafkaConsumer);
    }

    @Override
    public final DataStream<Customer> readCustomersCdcStream()
            throws RuntimeException {
        final Properties properties = KafkaProperties.getProperties(params);

        final FlinkKafkaConsumer<Customer> cKafkaConsumer =
                new FlinkKafkaConsumer<>(params.getRequired(KAFKA_SOURCE_TOPIC_2), cDeserializationSchema, properties);

        setFlinkKafkaConsumerOffsetStrategy(params, cKafkaConsumer);

        return env.addSource(cKafkaConsumer);
    }

    @Override
    public final void writeEnrichedTransactionsOutput(final DataStream<EnrichedTransaction> enrichedTrxStream)
            throws RuntimeException {
        final Properties properties = KafkaProperties.getProperties(params);

        final FlinkKafkaProducer<EnrichedTransaction> etxKafkaProducer = new FlinkKafkaProducer<>(
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
        final String checkpointPath = params.getRequired(CHECKPOINT_PATH);
        final StateBackend stateBackend = new FsStateBackend(checkpointPath);
        final Configuration conf = new Configuration();

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
    public static void main(final String[] args) throws Exception {
        params = ParameterUtils.parseArgs(args);

        new KafkaTransactionsEnrichmentStreamingJob()
                .createApplicationPipeline()
                .execute("Kafka Transactions Stream Enrichment");
    }
}
