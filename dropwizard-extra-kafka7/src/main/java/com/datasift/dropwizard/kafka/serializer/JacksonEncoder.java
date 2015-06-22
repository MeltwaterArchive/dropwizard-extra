package com.datasift.dropwizard.kafka.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.message.Message;
import kafka.serializer.Encoder;

/**
 * A Kafka {@link Encoder} for encoding an arbitrary type to a JSON blob.
 */
public class JacksonEncoder<T> implements Encoder<T> {

    private final ObjectMapper mapper;

    public JacksonEncoder(final ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public Message toMessage(final T event) {
        try {
            return new Message(mapper.writeValueAsBytes(event));
        } catch (final JsonProcessingException ex) {
            throw new RuntimeException(ex);
        }
    }
}
