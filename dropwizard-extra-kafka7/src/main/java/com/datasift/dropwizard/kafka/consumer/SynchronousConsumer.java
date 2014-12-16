package com.datasift.dropwizard.kafka.consumer;

import io.dropwizard.lifecycle.Managed;
import io.dropwizard.lifecycle.ServerLifecycleListener;
import io.dropwizard.util.Duration;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.Decoder;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A {@link KafkaConsumer} that processes messages synchronously using an {@link ExecutorService}.
 */
public class SynchronousConsumer<T> implements KafkaConsumer, Managed, ServerLifecycleListener {

    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private final ConsumerConnector connector;
    private final Map<String, Integer> partitions;
    private final ScheduledExecutorService executor;
    private final Decoder<T> decoder;
    private final StreamProcessor<T> processor;
    private final Duration initialRecoveryDelay;
    private final Duration maxRecoveryDelay;
    private final Duration retryResetDelay;
    private final int maxRecoveryAttempts;
    private final boolean shutdownOnFatal;
    private final Duration startDelay;

    private Server server = null;
    private boolean fatalErrorOccurred = false;

    // a thread to asynchronously handle unrecoverable errors in the stream consumer
    private final Thread shutdownThread = new Thread("kafka-unrecoverable-error-handler"){
        public void run() {
            while (true) {
                try {
                    Thread.sleep(10000);
                } catch (final InterruptedException e) {
                    // stop sleeping
                }
                if (fatalErrorOccurred) {
                    try {
                        if (shutdownOnFatal && server != null) {
                            // shutdown the full service
                            // note: shuts down the consumer as it's Managed by the Environment
                            server.stop();
                        } else {
                            // just shutdown the consumer
                            SynchronousConsumer.this.stop();
                        }
                    } catch (Exception e) {
                        LOG.error("Error occurred while attempting emergency shut down.", e);
                    }
                }
            }
        }
    };

    /**
     * Creates a {@link SynchronousConsumer} to process a stream.
     *
     * @param connector the {@link ConsumerConnector} of the underlying consumer.
     * @param partitions a mapping of the topic -> partitions to consume.
     * @param decoder a {@link Decoder} for decoding each message before being processed.
     * @param processor a {@link StreamProcessor} for processing messages.
     * @param executor the {@link ExecutorService} to process the stream with.
     */
    public SynchronousConsumer(final ConsumerConnector connector,
                               final Map<String, Integer> partitions,
                               final Decoder<T> decoder,
                               final StreamProcessor<T> processor,
                               final ScheduledExecutorService executor,
                               final Duration initialRecoveryDelay,
                               final Duration maxRecoveryDelay,
                               final Duration retryResetDelay,
                               final int maxRecoveryAttempts,
                               final boolean shutdownOnFatal,
                               final Duration startDelay) {
        this.connector = connector;
        this.partitions = partitions;
        this.decoder = decoder;
        this.processor = processor;
        this.executor = executor;
        this.initialRecoveryDelay = initialRecoveryDelay;
        this.maxRecoveryDelay = maxRecoveryDelay;
        this.retryResetDelay = retryResetDelay;
        this.maxRecoveryAttempts = maxRecoveryAttempts;
        this.shutdownOnFatal = shutdownOnFatal;
        this.startDelay = startDelay;

        shutdownThread.setDaemon(true);
        shutdownThread.start();
    }

    /**
     * Commits the currently consumed offsets.
     */
    public void commitOffsets() {
        connector.commitOffsets();
    }

    @Override
    public void serverStarted(final Server server) {
        this.server = server;
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
        final Set<Map.Entry<String, List<KafkaStream<T>>>> streams =
                connector.createMessageStreams(partitions, decoder).entrySet();

        for (final Map.Entry<String, List<KafkaStream<T>>> e : streams) {
            final String topic = e.getKey();
            final List<KafkaStream<T>> messageStreams = e.getValue();

            LOG.info("Consuming from topic '{}' with {} threads", topic, messageStreams.size());

            for (final KafkaStream<T> stream : messageStreams) {
                executor.schedule(
                        new StreamProcessorRunnable(topic, stream),
                        startDelay.getQuantity(),
                        startDelay.getUnit());
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
        LOG.trace("Shutting down Connector");
        connector.shutdown();
        LOG.trace("Connector shut down");
    }

    /**
     * Determines if this {@link KafkaConsumer} is currently consuming.
     *
     * @return true if this {@link KafkaConsumer} is currently consuming from at least one
     *         partition; otherwise, false.
     */
    public boolean isRunning() {
        return !executor.isShutdown() && !executor.isTerminated() && !fatalErrorOccurred;
    }

    private void fatalError() {
        this.fatalErrorOccurred = true;
        this.shutdownThread.interrupt();
    }

    /**
     * A {@link Runnable} that processes a {@link KafkaStream}.
     *
     * The configured {@link StreamProcessor} is used to process the stream.
     */
    private class StreamProcessorRunnable implements Runnable {

        private final KafkaStream<T> stream;
        private final String topic;
        private int attempts = 0;
        private long lastErrorTimestamp = 0;

        /**
         * Creates a {@link StreamProcessorRunnable} for the given topic and stream.
         *
         * @param topic the topic the {@link KafkaStream} belongs to.
         * @param stream a stream of {@link kafka.message.Message}s in the topic.
         */
        public StreamProcessorRunnable(final String topic, final KafkaStream<T> stream) {
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
            } catch (final Throwable e) {
                error(e);
            }
        }

        private void recoverableError(final Exception e) {
            LOG.warn("Error processing stream, restarting stream consumer ({} attempts remaining): {}",
                    maxRecoveryAttempts - attempts, e.toString());

            // reset attempts if there hasn't been a failure in a while
            if (System.currentTimeMillis() - lastErrorTimestamp >= retryResetDelay.toMilliseconds()) {
                attempts = 0;
            }

            // if a ceiling has been set on the number of retries, check if we have reached the ceiling
            attempts++;
            if (maxRecoveryAttempts > -1 && attempts >= maxRecoveryAttempts) {
                LOG.warn("Failed to restart consumer after {} retries", maxRecoveryAttempts);
                error(e);
            } else {
                try {
                    final long sleepTime = Math.min(
                            maxRecoveryDelay.toMilliseconds(),
                            (long) (initialRecoveryDelay.toMilliseconds() * Math.pow( 2, attempts)));

                    Thread.sleep(sleepTime);
                } catch(final InterruptedException ie){
                    LOG.warn("Error recovery grace period interrupted.", ie);
                }
                lastErrorTimestamp = System.currentTimeMillis();
                if (!executor.isShutdown()) {
                    executor.execute(this);
                }
            }
        }

        private void error(final Throwable e) {
            LOG.error("Unrecoverable error processing stream, shutting down", e);
            fatalError();
        }
    }
}
