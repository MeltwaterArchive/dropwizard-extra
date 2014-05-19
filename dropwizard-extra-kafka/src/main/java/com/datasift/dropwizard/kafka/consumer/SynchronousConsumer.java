package com.datasift.dropwizard.kafka.consumer;

import io.dropwizard.lifecycle.Managed;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.Decoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * A {@link KafkaConsumer} that processes messages synchronously using an {@link ExecutorService}.
 */
public class SynchronousConsumer<K, V> implements KafkaConsumer, Managed {

    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private final ConsumerConnector connector;
    private final Map<String, Integer> partitions;
    private final ExecutorService executor;
    private final Decoder<K> keyDecoder;
    private final Decoder<V> valueDecoder;
    private final StreamProcessor<K, V> processor;

    /**
     * Creates a {@link SynchronousConsumer} to process a stream.
     *
     * @param connector the {@link ConsumerConnector} of the underlying consumer.
     * @param partitions a mapping of the topic -> partitions to consume.
     * @param keyDecoder a {@link Decoder} for decoding the key of each message before being processed.
     * @param valueDecoder a {@link Decoder} for decoding each message before being processed.
     * @param processor a {@link StreamProcessor} for processing messages.
     * @param executor the {@link ExecutorService} to process the stream with.
     */
    public SynchronousConsumer(final ConsumerConnector connector,
                               final Map<String, Integer> partitions,
                               final Decoder<K> keyDecoder,
                               final Decoder<V> valueDecoder,
                               final StreamProcessor<K, V> processor,
                               final ExecutorService executor) {
        this.connector = connector;
        this.partitions = partitions;
        this.keyDecoder = keyDecoder;
        this.valueDecoder = valueDecoder;
        this.processor = processor;
        this.executor = executor;
    }

    /**
     * Commits the currently consumed offsets.
     */
    public void commitOffsets() {
        connector.commitOffsets();
    }

    /**
     * Starts this {@link SynchronousConsumer} immediately.
     * <p/>
     * The consumer will immediately begin consuming from the configured topics using the configured
     * {@link Decoder} to decode messages and {@link StreamProcessor} to process the decoded
     * messages.
     * <p/>
     * Each partition will be consumed using a separate thread.
     *
     * @throws Exception if an error occurs starting the consumer
     */
    @Override
    public void start() throws Exception {
        final Set<Map.Entry<String, List<KafkaStream<K, V>>>> streams =
                connector.createMessageStreams(partitions, keyDecoder, valueDecoder).entrySet();

        for (final Map.Entry<String, List<KafkaStream<K, V>>> e : streams) {
            final String topic = e.getKey();
            final List<KafkaStream<K, V>> messageStreams = e.getValue();

            LOG.info("Consuming from topic '{}' with {} threads", topic, messageStreams.size());

            for (final KafkaStream<K, V> stream : messageStreams) {
                executor.execute(new StreamProcessorRunnable(topic, stream));
            }
        }
    }

    /**
     * Stops this {@link SynchronousConsumer} immediately.
     *
     * @throws Exception
     */
    @Override
    public void stop() throws Exception {
        connector.shutdown();
    }

    /**
     * Determines if this {@link KafkaConsumer} is currently consuming.
     *
     * @return true if this {@link KafkaConsumer} is currently consuming from at least one
     *         partition; otherwise, false.
     */
    public boolean isRunning() {
        return !executor.isShutdown() && !executor.isTerminated();
    }

    /**
     * A {@link Runnable} that processes a {@link KafkaStream}.
     *
     * The configured {@link StreamProcessor} is used to process the stream.
     */
    private class StreamProcessorRunnable implements Runnable {

        private final KafkaStream<K, V> stream;
        private final String topic;

        /**
         * Creates a {@link StreamProcessorRunnable} for the given topic and stream.
         *
         * @param topic the topic the {@link KafkaStream} belongs to.
         * @param stream a stream of {@link kafka.message.Message}s in the topic.
         */
        public StreamProcessorRunnable(final String topic, final KafkaStream<K, V> stream) {
            this.topic = topic;
            this.stream = stream;
        }

        /**
         * Process the stream using the configured {@link StreamProcessor}.
         * <p/>
         * If an {@link Exception} is thrown during processing, if it is deemed <i>recoverable</i>,
         * the stream will continue to be consumed.
         * <p/>
         * Unrecoverable {@link Exception}s will cause the consumer to shut down completely.
         */
        @Override
        public void run() {
            try {
                processor.process(stream, topic);
            } catch (final IllegalStateException e) {
                error(e);
            } catch (final Exception e) {
                recoverableError(e);
            }
        }

        private void recoverableError(final Exception e) {
            LOG.warn("Error processing stream, restarting stream consumer", e);
            executor.execute(this);
        }

        private void error(final Exception e) {
            LOG.error("Unrecoverable error processing stream, shutting down", e);
            try {
                stop();
            } catch (final Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
