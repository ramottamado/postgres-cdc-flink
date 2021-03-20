package dev.ramottamado.java.flink.util.serialization;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.ramottamado.java.flink.schema.EnrichedTransactions;

public class EnrichedTransactionsJSONSerializationSchema implements KafkaSerializationSchema<EnrichedTransactions> {

    private final static long serialVersionUID = -102983L;
    private String topic;
    private ObjectMapper mapper;
    private final static Logger logger = LoggerFactory.getLogger(EnrichedTransactionsJSONSerializationSchema.class);

    public EnrichedTransactionsJSONSerializationSchema(String topic) {

        super();
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(EnrichedTransactions element, Long timestamp) {

        byte[] message = null;

        if (mapper == null) {
            mapper = new ObjectMapper();
        }

        try {
            message = mapper.writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            logger.error("Error processing JSON", e);
        }

        return new ProducerRecord<byte[], byte[]>(topic, message);
    }
}
