package dev.ramottamado.java.flink;

import static dev.ramottamado.java.flink.config.ParameterConfig.DEBUG_RESULT_STREAM;
import static dev.ramottamado.java.flink.config.ParameterConfig.KAFKA_OFFSET_STRATEGY;
import static dev.ramottamado.java.flink.config.ParameterConfig.KAFKA_SOURCE_TOPIC_1;
import static dev.ramottamado.java.flink.config.ParameterConfig.KAFKA_SOURCE_TOPIC_2;
import static dev.ramottamado.java.flink.config.ParameterConfig.KAFKA_TARGET_TOPIC;

import java.util.Properties;

import org.apache.flink.api.java.utils.ParameterTool;
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
import dev.ramottamado.java.flink.util.serialization.EnrichedTransactionsJSONSerializationSchema;

/**
 * The class {@link KafkaTransactionsEnrichmentStreamingJob} provides {@link TransactionsEnrichmentStreamingJob}
 * for enriching {@link Transactions} data using Flink.
 *
 * @see TransactionsEnrichmentStreamingJob
 */
public class KafkaTransactionsEnrichmentStreamingJob extends TransactionsEnrichmentStreamingJob {
    private DebeziumJSONEnvelopeDeserializationSchema<TransactionsBean> tDeserializationSchema =
            new DebeziumJSONEnvelopeDeserializationSchema<>(TransactionsBean.class);

    private DebeziumJSONEnvelopeDeserializationSchema<CustomersBean> cDeserializationSchema =
            new DebeziumJSONEnvelopeDeserializationSchema<>(CustomersBean.class);

    private EnrichedTransactionsJSONSerializationSchema etxSerializationSchema =
            new EnrichedTransactionsJSONSerializationSchema("enriched_transactions");

    @Override
    public final DataStream<TransactionsBean> readTransactionsCdcStream(
            StreamExecutionEnvironment env, ParameterTool params) throws RuntimeException {
        Properties properties = KafkaProperties.getProperties(params);

        FlinkKafkaConsumer<TransactionsBean> tKafkaConsumer = new FlinkKafkaConsumer<>(
                params.getRequired(KAFKA_SOURCE_TOPIC_1),
                tDeserializationSchema,
                properties);

        String kafkaOffsetStrategy = params.get(KAFKA_OFFSET_STRATEGY, "inherit");

        if (kafkaOffsetStrategy == "earliest") {
            tKafkaConsumer.setStartFromEarliest();
        } else if (kafkaOffsetStrategy == "latest") {
            tKafkaConsumer.setStartFromLatest();
        } else {
            tKafkaConsumer.setStartFromGroupOffsets();
        }

        return env.addSource(tKafkaConsumer);
    }

    @Override
    public final DataStream<CustomersBean> readCustomersCdcStream(StreamExecutionEnvironment env, ParameterTool params)
            throws RuntimeException {
        Properties properties = KafkaProperties.getProperties(params);

        FlinkKafkaConsumer<CustomersBean> cKafkaConsumer =
                new FlinkKafkaConsumer<>(params.getRequired(KAFKA_SOURCE_TOPIC_2), cDeserializationSchema, properties);

        String kafkaOffsetStrategy = params.get(KAFKA_OFFSET_STRATEGY, "inherit");

        if (kafkaOffsetStrategy == "earliest") {
            cKafkaConsumer.setStartFromEarliest();
        } else if (kafkaOffsetStrategy == "latest") {
            cKafkaConsumer.setStartFromLatest();
        } else {
            cKafkaConsumer.setStartFromGroupOffsets();
        }

        return env.addSource(cKafkaConsumer);
    }

    @Override
    public final void writeEnrichedTransactionsOutput(
            DataStream<EnrichedTransactionsBean> enrichedTrxStream, ParameterTool params) throws RuntimeException {
        Properties properties = KafkaProperties.getProperties(params);

        FlinkKafkaProducer<EnrichedTransactionsBean> etxKafkaProducer = new FlinkKafkaProducer<>(
                params.get(KAFKA_TARGET_TOPIC, "enriched_transactions"),
                etxSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

        if (params.getBoolean(DEBUG_RESULT_STREAM, false) == true)
            enrichedTrxStream.map(new EnrichedTransactionsToStringMapFunction()).print();

        enrichedTrxStream.addSink(etxKafkaProducer);
    }

    /**
     * Main method to run the application.
     *
     * @param  args      the arguments to pass into the application
     * @throws Exception if some errors happened
     */
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterUtils.parseArgs(args);

        new KafkaTransactionsEnrichmentStreamingJob()
                .createApplicationPipeline(params)
                .execute("Kafka Transactions Stream Enrichment");
    }
}
