package com.datasift.dropwizard.kafka.serializer;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import kafka.message.Message;
import kafka.serializer.Decoder;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A Kafka {@link Decoder} for decoding an arbitrary type from a JSON blob.
 */
public class JacksonDecoder<T> implements Decoder<T> {

    private final Class<T> clazz;
    private final ObjectMapper mapper;

    public JacksonDecoder(final ObjectMapper mapper, final Class<T> clazz) {
        this.mapper = mapper;
        this.clazz = clazz;
    }

    @Override
    public T toEvent(final Message msg) {
        try {
            try {
                final ByteBuffer bb = msg.payload();
                if (bb.hasArray()) {
                    return mapper.readValue(bb.array(), bb.arrayOffset(), bb.limit() - bb.position(), clazz);
                } else {
                    return mapper.readValue(new ByteBufferBackedInputStream(bb), clazz);
                }
            } catch (final JsonParseException ex) {
                final JsonLocation location = ex.getLocation();
                Object src = location.getSourceRef();
                if (src instanceof ByteBuffer) {
                    src = ((ByteBuffer) src).asCharBuffer();
                } else if (src instanceof byte[]) {
                    src = new String((byte[]) src);
                } else if (src instanceof char[]) {
                    src = new String((char[]) src);
                }
                throw new JsonParseException(
                        ex.getMessage(),
                        new JsonLocation(
                                src,
                                location.getByteOffset(),
                                location.getCharOffset(),
                                location.getLineNr(),
                                location.getColumnNr()),
                        ex.getCause());
            }
        } catch (final IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
