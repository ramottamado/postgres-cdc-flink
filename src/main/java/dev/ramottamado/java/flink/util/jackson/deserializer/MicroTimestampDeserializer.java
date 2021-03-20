package dev.ramottamado.java.flink.util.jackson.deserializer;

import java.io.IOException;
import java.time.Instant;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MicroTimestampDeserializer extends StdDeserializer<Instant> {

    public MicroTimestampDeserializer() {

        this(null);
    }

    public MicroTimestampDeserializer(Class<?> vc) {

        super(vc);
    }

    private final static long serialVersionUID = 8178417124781L;

    private final static Logger logger = LoggerFactory.getLogger(MicroTimestampDeserializer.class);

    @Override
    public Instant deserialize(JsonParser jp, DeserializationContext ctx) throws IOException {

        Long timestamp = jp.getLongValue() / 1000000;

        try {
            return Instant.ofEpochSecond(timestamp, 0);
        } catch (Exception e) {
            logger.error("Local date is not valid.", e);

            return null;
        }
    }
}
