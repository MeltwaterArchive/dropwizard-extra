package com.datasift.dropwizard.kafka.consumer;

import kafka.consumer.KafkaMessageStream;

/**
 * Processes messages of type {@code T} from a Kafka
 * {@link kafka.consumer.KafkaMessageStream}.
 *
 * @param <T> the decoded type of the message to process
 */
public abstract class MessageProcessor<T> implements StreamProcessor<T> {

    /**
     * Processes a {@code message} of type {@code T}.
     *
     * @param message the message to process
     * @param topic   the topic the message belongs to
     */
    abstract public void process(T message, String topic);

    /**
     * Processes a {@link KafkaMessageStream} by iteratively processing each
     * message separately.
     *
     * @param stream the {@link KafkaMessageStream} of messages to process
     * @param topic  the topic the {@code stream} belongs to
     *
     * @see StreamProcessor#process(kafka.consumer.KafkaMessageStream, String)
     */
    public void process(KafkaMessageStream<T> stream, String topic) {
        for (T message : stream) {
            process(message, topic);
        }
    }
}
