package dev.ramottamado.java.flink.util.jackson.serializer;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

public class TimestampSerializer extends StdSerializer<Instant> {

    private static final long serialVersionUID = 123718191L;

    public TimestampSerializer() {

        this(null);
    }

    public TimestampSerializer(Class<Instant> type) {

        super(type);
    }

    @Override
    public void serialize(Instant value, JsonGenerator jg, SerializerProvider sp)
            throws IOException, DateTimeException {

        DateTimeFormatter formatter = DateTimeFormatter
                .ofPattern("yyyy-MM-dd HH:mm:ss")
                .withZone(ZoneId.of("Z"));
        String parsedTimestamp = formatter.format(value);

        jg.writeString(parsedTimestamp);
    }
}
