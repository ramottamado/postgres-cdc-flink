package dev.ramottamado.java.flink.util.jackson.serializer;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

public class TimestampSerializer extends StdSerializer<LocalDateTime> {
    private final static long serialVersionUID = 123718191L;

    public TimestampSerializer() {
        this(null);
    }

    public TimestampSerializer(Class<LocalDateTime> x) {
        super(x);
    }

    @Override
    public void serialize(LocalDateTime value, JsonGenerator jg, SerializerProvider sp)
            throws IOException, JsonProcessingException {
        String parsedLocalDateTime = value.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        jg.writeString(parsedLocalDateTime);
    }
}
