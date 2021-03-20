package dev.ramottamado.java.flink;

import static dev.ramottamado.java.flink.config.ParameterConfig.DEBUG_RESULT_STREAM;
import static dev.ramottamado.java.flink.config.ParameterConfig.KAFKA_SOURCE_TOPIC_1;
import static dev.ramottamado.java.flink.config.ParameterConfig.KAFKA_SOURCE_TOPIC_2;

import java.util.Properties;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import dev.ramottamado.java.flink.functions.EnrichedTransactionsToStringMapFunction;
import dev.ramottamado.java.flink.schema.Customers;
import dev.ramottamado.java.flink.schema.EnrichedTransactions;
import dev.ramottamado.java.flink.schema.Transactions;
import dev.ramottamado.java.flink.util.ParameterUtils;
import dev.ramottamado.java.flink.util.kafka.KafkaProperties;
import dev.ramottamado.java.flink.util.serialization.DebeziumJSONEnvelopeDeserializationSchema;
import dev.ramottamado.java.flink.util.serialization.EnrichedTransactionsJSONSerializationSchema;

public class KafkaTransactionsEnrichmentStreamingJob extends TransactionsEnrichmentStreamingJob {

    private DebeziumJSONEnvelopeDeserializationSchema<Transactions> tDeserializationSchema =
            new DebeziumJSONEnvelopeDeserializationSchema<>(Transactions.class);

    private DebeziumJSONEnvelopeDeserializationSchema<Customers> cDeserializationSchema =
            new DebeziumJSONEnvelopeDeserializationSchema<>(Customers.class);

    private EnrichedTransactionsJSONSerializationSchema etxSerializationSchema =
            new EnrichedTransactionsJSONSerializationSchema("enriched_transactions");

    @Override
    public final DataStream<Transactions> readTransactionsCdcStream(
            StreamExecutionEnvironment env, ParameterTool params
    ) throws Exception {

        Properties properties = KafkaProperties.getProperties(params);

        FlinkKafkaConsumer<Transactions> tKafkaConsumer = new FlinkKafkaConsumer<>(
                params.getRequired(KAFKA_SOURCE_TOPIC_1), tDeserializationSchema, properties
        );

        tKafkaConsumer.setStartFromEarliest();

        return env.addSource(tKafkaConsumer);
    }

    @Override
    public final DataStream<Customers> readCustomersCdcStream(StreamExecutionEnvironment env, ParameterTool params)
            throws Exception {

        Properties properties = KafkaProperties.getProperties(params);

        FlinkKafkaConsumer<Customers> cKafkaConsumer = new FlinkKafkaConsumer<>(
                params.getRequired(KAFKA_SOURCE_TOPIC_2), cDeserializationSchema, properties
        );

        cKafkaConsumer.setStartFromEarliest();

        return env.addSource(cKafkaConsumer);
    }

    @Override
    public final void writeEnrichedTransactionsOutput(
            DataStream<EnrichedTransactions> enrichedTrxStream, ParameterTool params
    ) throws Exception {

        Properties properties = KafkaProperties.getProperties(params);

        FlinkKafkaProducer<EnrichedTransactions> etxKafkaProducer = new FlinkKafkaProducer<>(
                "enriched_transactions", etxSerializationSchema, properties,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );

        if (params.getBoolean(DEBUG_RESULT_STREAM, false) == true)
            enrichedTrxStream.map(new EnrichedTransactionsToStringMapFunction()).print();

        enrichedTrxStream.addSink(etxKafkaProducer);
    }

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterUtils.parseArgs(args);

        new KafkaTransactionsEnrichmentStreamingJob()
                .createApplicationPipeline(params)
                .execute();

    }

}
