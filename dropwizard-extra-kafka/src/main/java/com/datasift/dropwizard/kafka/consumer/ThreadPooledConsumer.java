package com.datasift.dropwizard.kafka.consumer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yammer.dropwizard.logging.Log;
import com.yammer.dropwizard.util.Duration;
import kafka.consumer.KafkaMessageStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.Decoder;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * A {@link KafkaConsumer} that processes messages using a fixed-sized
 * {@link ThreadPoolExecutor}.
 *
 * The number of threads used for the {@link ThreadPoolExecutor} is determined
 * automatically by the total number of partitions this {@link KafkaConsumer}
 * is configured to consume.
 */
public class ThreadPooledConsumer<T> implements KafkaConsumer<T> {

    private final Log LOG = Log.forClass(getClass());

    private final ConsumerConnector connector;
    private final Map<String, Integer> partitions;
    private final ExecutorService executor;
    private final Duration shutdownPeriod;
    private final Decoder<T> decoder;
    private final StreamProcessor<T> processor;

    /**
     * Creates a {@link ThreadPooledConsumer} to process a stream.
     *
     * @param connector      the {@link ConsumerConnector} of the underlying
     *                       consumer
     * @param partitions     a mapping of the topic -> partitions to consume
     * @param shutdownPeriod the time to wait on a graceful shutdown before
     *                       forcibly stopping the consumer
     * @param decoder        a {@link Decoder} for decoding each
     *                       {@link kafka.message.Message} to type {@code T}
     *                       before being processed
     * @param processor      a {@link StreamProcessor} for processing messages
     *                       of type {@code T}
     * @param name           the name of this {@link KafkaConsumer}, used to
     *                       control the name of the underlying thread-pool
     */
    public ThreadPooledConsumer(final ConsumerConnector connector,
                                final Map<String, Integer> partitions,
                                final Duration shutdownPeriod,
                                final Decoder<T> decoder,
                                final StreamProcessor<T> processor,
                                final String name) {
        this.connector = connector;
        this.partitions = partitions;
        this.shutdownPeriod = shutdownPeriod;
        this.decoder = decoder;
        this.processor = processor;

        // custom ThreadFactory to customize the Thread names
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat(String.format("kafka-consumer-%s-%%d", name))
                .build();

        this.executor = Executors.newFixedThreadPool(getThreads(), threadFactory);
    }

    /**
     * Gets the number of {@link Thread}s this {@link ThreadPooledConsumer} will
     * use.
     *
     * @return the number of {@link Thread} to consume with
     */
    public int getThreads() {
        int threads = 0;
        for (final Integer p : partitions.values()) {
            threads = threads + p;
        }
        return threads;
    }

    /**
     * Commits the currently consumed offsets.
     */
    public void commitOffsets() {
        connector.commitOffsets();
    }

    /**
     * Starts this {@link ThreadPooledConsumer} immediately.
     *
     * The consumer will immediately begin consuming from the configured topics
     * using the configured {@link Decoder} to decode messages and
     * {@link StreamProcessor} to process the decoded messages.
     *
     * Each partition will be consumed using a separate thread.
     *
     * @throws Exception if an error occurs starting the consumer
     */
    @Override
    public void start() throws Exception {
        final Set<Map.Entry<String, List<KafkaMessageStream<T>>>> streams =
                connector.createMessageStreams(partitions, decoder).entrySet();

        for (final Map.Entry<String, List<KafkaMessageStream<T>>> e : streams) {
            final String topic = e.getKey();
            final List<KafkaMessageStream<T>> messageStreams = e.getValue();

            LOG.info("Consuming from topic '{}' with {} threads",
                    topic, messageStreams.size());

            for (final KafkaMessageStream<T> stream : messageStreams) {
                executor.submit(new StreamProcessorRunnable(topic, stream));
            }
        }
    }

    /**
     * Stops this {@link ThreadPooledConsumer} immediately.
     *
     * The underlying {@link java.util.concurrent.ExecutorService}
     *
     * @throws Exception
     */
    @Override
    public void stop() throws Exception {
        executor.shutdownNow();
        executor.awaitTermination(
                shutdownPeriod.toNanoseconds(),
                TimeUnit.NANOSECONDS);
        connector.shutdown();
    }

    /**
     * Determines if this {@link KafkaConsumer} is currently consuming.
     *
     * @return true if this {@link KafkaConsumer} is currently consuming from
     *         at least one partition; otherwise, false
     */
    public boolean isRunning() {
        return !executor.isShutdown() &&
                !executor.isTerminated();
    }

    /**
     * A {@link Runnable} that processes a {@link KafkaMessageStream}.
     *
     * The configured {@link StreamProcessor} is used to process the stream.
     */
    private class StreamProcessorRunnable implements Runnable {

        private final KafkaMessageStream<T> stream;
        private final String topic;

        /**
         * Creates a {@link StreamProcessorRunnable} for the given topic and
         * stream.
         *
         * @param topic  the topic the {@link KafkaMessageStream} belongs to
         * @param stream a stream of {@link kafka.message.Message}s in the topic
         */
        public StreamProcessorRunnable(final String topic,
                                       final KafkaMessageStream<T> stream) {
            this.topic = topic;
            this.stream = stream;
        }

        @Override
        public void run() {
            try {
                processor.process(stream, topic);
            } catch (final Exception t) {
                // only handle the error if the Thread hasn't been interrupted
                if (!Thread.currentThread().isInterrupted()) {
                    handleError(t);
                }
            }
        }

        /**
         * Handles an {@link Exception} that has occurred.
         *
         * @param e the {@link Exception} to handle
         */
        private void handleError(final Exception e) {
            if (isRecoverable(e)) {
                LOG.warn(e,
                        "Error processing stream, restarting stream consumer");
                executor.submit(this);
            } else {
                LOG.error(e,
                        "Unrecoverable error processing stream, shutting down");
                try {
                    stop();
                } catch (final Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        }

        /**
         * Determines whether the given {@link Exception} can be recovered from.
         *
         * @param t the {@link Exception} to test
         * @return true if the {@link Exception} is recoverable; false if it is
         *         not recoverable
         */
        private boolean isRecoverable(final Exception t) {
            return !(t instanceof IllegalStateException);
        }
    }
}
