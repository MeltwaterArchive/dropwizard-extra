package com.datasift.dropwizard.kafka.consumer;

import kafka.consumer.KafkaMessageStream;

/**
 * Processes a {@link KafkaMessageStream} of messages of type {@code T}.
 *
 * Note: since consumers may use multiple threads, it is important that
 * implementations are thread-safe.
 *
 * If you wish to process each message individually and iteratively, it's advised
 * that you instead use a {@link MessageProcessor}, as it provides a higher-level
 * of abstraction.
 */
public interface StreamProcessor<T> {

    /**
     * Process a {@link KafkaMessageStream} of messages of type T.
     *
     * @param stream the {@link KafkaMessageStream} of messages to process
     * @param topic the topic the {@code stream} belongs to
     */
    public void process(KafkaMessageStream<T> stream, String topic);
}
