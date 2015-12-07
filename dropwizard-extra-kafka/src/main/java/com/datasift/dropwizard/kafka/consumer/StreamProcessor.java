package com.datasift.dropwizard.kafka.consumer;

import kafka.message.MessageAndMetadata;

/**
 * Processes an {@link Iterable} of messages of type {@code T}.
 * <p>
 * If you wish to process each message individually and iteratively, it's advised that you instead
 * use a {@link MessageProcessor}, as it provides a higher-level of abstraction.
 * <p>
 * <i>Note: since consumers may use multiple threads, it is important that implementations are
 * thread-safe.</i>
 */
public interface StreamProcessor<K, V> {

    /**
     * Process an {@link Iterable} of messages of type T.
     *
     * @param stream the stream of messages to process.
     * @param topic the topic the {@code stream} belongs to.
     */
    public void process(Iterable<MessageAndMetadata<K, V>> stream, String topic);
}
