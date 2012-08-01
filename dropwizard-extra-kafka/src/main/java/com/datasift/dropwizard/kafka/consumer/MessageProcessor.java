package com.datasift.dropwizard.kafka.consumer;

/**
 * Processes messages of type {@code T} from a Kafka message stream.
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
     * Processes a {@link Iterable} by iteratively processing each message.
     *
     * @param stream the stream of messages to process
     * @param topic  the topic the {@code stream} belongs to
     *
     * @see StreamProcessor#process(Iterable, String)
     */
    public void process(Iterable<T> stream, String topic) {
        for (T message : stream) {
            process(message, topic);
        }
    }
}
