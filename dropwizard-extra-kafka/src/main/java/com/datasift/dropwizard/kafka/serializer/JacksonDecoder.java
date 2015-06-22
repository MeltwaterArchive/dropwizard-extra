package com.datasift.dropwizard.kafka.serializer;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    public T fromBytes(final byte[] bytes) {
        try {
            try {
                return mapper.readValue(bytes, clazz);
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
