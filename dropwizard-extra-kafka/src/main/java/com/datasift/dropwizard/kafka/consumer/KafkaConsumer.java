package com.datasift.dropwizard.kafka.consumer;

import com.yammer.dropwizard.lifecycle.Managed;
import kafka.message.Message;
import kafka.serializer.Decoder;

/**
 * Interface for consuming a stream of {@link Message}s from Kafka.
 */
public interface KafkaConsumer extends Managed {

    /**
     * Consumes a raw stream of {@link Message}s.
     *
     * It is recommended that you define a {@link Decoder} and use it to process
     * messages using {@link KafkaConsumer#consume(StreamProcessor, kafka.serializer.Decoder)} )}
     * instead.
     *
     * @param processor a {@link StreamProcessor} to process the raw message stream
     */
    public void consume(StreamProcessor<Message> processor);

    /**
     * Consumes a stream of messages of type T.
     *
     * The messages will first be decoded from {@link Message] to type {@link T}
     * by the specified {@link Decoder} instance.
     *
     * @param processor a {@link StreamProcessor} to process the message stream
     * @param decoder a {@link Decoder} to decode the messages from {@link Message} to type {@link T}
     * @param <T> the type of the decoded messages
     */
    public <T> void consume(StreamProcessor<T> processor, Decoder<T> decoder);

    /**
     * Commit the offsets of the current position in the message streams.
     *
     * @see kafka.consumer.ConsumerConnector#commitOffsets()
     */
    public void commitOffsets();
}
