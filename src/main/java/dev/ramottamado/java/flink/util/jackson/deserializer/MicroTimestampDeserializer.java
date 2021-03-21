package dev.ramottamado.java.flink.util.jackson.deserializer;

import java.io.IOException;
import java.time.Instant;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link MicroTimestampDeserializer} allows for deserializing Debezium {@code MicroTimestamp} into
 * {@link java.time.Instant}.
 */
public class MicroTimestampDeserializer extends StdDeserializer<Instant> {
    private static final long serialVersionUID = 8178417124781L;
    private static final Logger logger = LoggerFactory.getLogger(MicroTimestampDeserializer.class);

    /**
     * The {@link MicroTimestampDeserializer} allows for deserializing Debezium {@code MicroTimestamp} into
     * {@link java.time.Instant}.
     */
    public MicroTimestampDeserializer() {
        this(null);
    }

    /**
     * The {@link MicroTimestampDeserializer} allows for deserializing Debezium {@code MicroTimestamp} into
     * {@link java.time.Instant}.
     *
     * @param vc the value class of serialized data
     */
    public MicroTimestampDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public Instant deserialize(JsonParser parser, DeserializationContext ctx) throws IOException {
        Long timestamp = parser.getLongValue() / 1000000;

        try {
            return Instant.ofEpochSecond(timestamp, 0);
        } catch (Exception e) {
            logger.error("Local date is not valid.", e);

            return null;
        }
    }
}
