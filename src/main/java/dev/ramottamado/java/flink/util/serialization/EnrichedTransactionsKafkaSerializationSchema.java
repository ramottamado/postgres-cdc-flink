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

package dev.ramottamado.java.flink.util.serialization;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.ramottamado.java.flink.annotation.PublicEvolving;
import dev.ramottamado.java.flink.schema.EnrichedTransaction;

/**
 * The {@link EnrichedTransactionsKafkaSerializationSchema} describes how to serialize {@link EnrichedTransaction}
 * into {@link ProducerRecord} for Apache Kafka.
 *
 * @author Tamado Sitohang
 * @since  1.0
 */
@PublicEvolving
public class EnrichedTransactionsKafkaSerializationSchema
        implements KafkaSerializationSchema<EnrichedTransaction> {
    private final static long serialVersionUID = -102983L;
    private final static Logger logger = LoggerFactory.getLogger(EnrichedTransactionsKafkaSerializationSchema.class);
    private final ObjectMapper mapper = new ObjectMapper();
    private final String topic;

    /**
     * The {@link EnrichedTransactionsKafkaSerializationSchema} describes how to serialize
     * {@link EnrichedTransaction} into {@link ProducerRecord} for Apache Kafka.
     *
     * @param  topic the Kafka topic to publish the resulting records
     * @author       Tamado Sitohang
     * @since        1.0
     */
    public EnrichedTransactionsKafkaSerializationSchema(final String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(final EnrichedTransaction element, final Long timestamp) {
        byte[] message = null;

        try {
            message = mapper.writeValueAsBytes(element);
        } catch (final JsonProcessingException e) {
            logger.error("Error processing JSON", e);
        }

        return new ProducerRecord<>(topic, message);
    }
}
