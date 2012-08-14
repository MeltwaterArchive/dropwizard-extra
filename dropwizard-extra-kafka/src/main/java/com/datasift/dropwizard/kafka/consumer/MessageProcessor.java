package com.datasift.dropwizard.kafka.consumer;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

/**
 * Processes messages of type {@code T} from a Kafka message stream.
 * <p/>
 * This {@link StreamProcessor} is instrumented with {@link Metric}s;
 * specifically, a {@link Timer} that tracks the time taken to process each
 * message in the stream.
 *
 * @param <T> the decoded type of the message to process
 */
public abstract class MessageProcessor<T> implements StreamProcessor<T> {

    /**
     * {@link Timer} for the processing of each message in the stream.
     */
    private final Timer processed;

    /**
     * Creates a MessageProcessor; registers {@link Metric}s with the
     * {@link Metrics#defaultRegistry() default registry}.
     */
    public MessageProcessor() {
        this(Metrics.defaultRegistry());
    }

    /**
     * Creates a MessageProcessor; registers {@link Metric}s with the given
     * {@link MetricsRegistry}.
     *
     * @param registry the {@link MetricsRegistry} to register metrics with
     */
    public MessageProcessor(final MetricsRegistry registry) {
        processed = registry.newTimer(getClass(), "processed");
    }

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
    public void process(final Iterable<T> stream, final String topic) {
        for (final T message : stream) {
            final TimerContext context = processed.time();
            process(message, topic);
            context.stop();
        }
    }
}
