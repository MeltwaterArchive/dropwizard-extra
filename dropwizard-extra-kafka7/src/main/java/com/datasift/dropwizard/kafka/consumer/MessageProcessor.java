package com.datasift.dropwizard.kafka.consumer;

import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;
import kafka.message.MessageAndMetadata;

/**
 * Processes messages of type {@code T} from a Kafka message stream.
 * <p/>
 * This {@link StreamProcessor} is instrumented with {@link Metric}s; specifically, a {@link Timer}
 * that tracks the time taken to process each message in the stream.
 *
 * @param <T> the decoded type of the message to process
 */
public abstract class MessageProcessor<T> implements StreamProcessor<T> {
//
//    /**
//     * {@link Timer} for the processing of each message in the stream.
//     */
//    private final Timer processed;
//
//    /**
//     * Creates a MessageProcessor; registers {@link Metric}s with the given {@link MetricRegistry}.
//     *
//     * @param registry the {@link MetricRegistry} to register metrics with.
//     * @param name the name to use for metrics of this processor.
//     */
//    public MessageProcessor(final MetricRegistry registry, final String name) {
//        processed = registry.timer(MetricRegistry.name(name, "processed"));
//    }

    /**
     * Processes a {@code message} of type {@code T}.
     *
     * @param key the key of the message to process.
     * @param message the message to process.
     * @param topic the topic the entry belongs to.
     */
    abstract public void process(T message, String topic);

    /**
     * Processes a {@link Iterable} by iteratively processing each message.
     *
     * @param stream the stream of messages to process.
     * @param topic the topic the {@code stream} belongs to.
     *
     * @see StreamProcessor#process(Iterable, String)
     */
    public void process(final Iterable<MessageAndMetadata<T>> stream, final String topic) {
        for (final MessageAndMetadata<T> entry : stream) {
//            final Timer.Context context = processed.time();
            process(entry.message(), topic);
//            context.stop();
        }
    }
}
