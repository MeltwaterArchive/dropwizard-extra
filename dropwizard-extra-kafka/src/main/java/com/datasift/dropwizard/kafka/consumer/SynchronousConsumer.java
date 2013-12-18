package com.datasift.dropwizard.kafka.consumer;

import com.yammer.dropwizard.lifecycle.Managed;
import com.yammer.dropwizard.util.Duration;
import kafka.consumer.KafkaMessageStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.Decoder;
import org.eclipse.jetty.util.component.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * A {@link KafkaConsumer} that processes messages synchronously using an {@link ExecutorService}.
 */
public class SynchronousConsumer<T> implements KafkaConsumer, Managed {

    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private final ConsumerConnector connector;
    private final Map<String, Integer> partitions;
    private final ExecutorService executor;
    private final Decoder<T> decoder;
    private final StreamProcessor<T> processor;
    private final Duration initialDelay;
    private final Duration maxDelay;
    private final Duration retryResetDelay;
    private final int maxRetries;
    private final boolean shutdownOnFatal;
    private boolean fatalErrorOccurred = false;
    private LifeCycle server;


    /**
     * Creates a {@link SynchronousConsumer} to process a stream.
     *
     * @param connector the {@link ConsumerConnector} of the underlying consumer.
     * @param partitions a mapping of the topic -> partitions to consume.
     * @param decoder a {@link Decoder} for decoding each {@link kafka.message.Message} to type
     *                {@code T} before being processed.
     * @param processor a {@link StreamProcessor} for processing messages of type {@code T}.
     * @param executor the {@link ExecutorService} to process the stream with.
     * @param initialDelay the initial {@link Duration} after which to attempt a recovery after an Exception.
     * @param maxDelay the maximum {@link Duration} to wait between recovery attempts.
     * @param retryResetDelay If no errors have occurred for this duration, the retry count is reverted to zero and the delay between retries is reset to initialDelay
     * @param maxRetries the maximum number of continuous recovery attempts before moving to an unrecoverable state. -1 indicates no upper limit to the number of retries.
     * @param shutdownOnFatal indicates whether to gracefully shut down the server in the event of an unrecoverable error.
     */
    public SynchronousConsumer(final ConsumerConnector connector,
                               final Map<String, Integer> partitions,
                               final Decoder<T> decoder,
                               final StreamProcessor<T> processor,
                               final ExecutorService executor,
                               final Duration initialDelay,
                               final Duration maxDelay,
                               final Duration retryResetDelay,
                               final int maxRetries,
                               final boolean shutdownOnFatal) {
        this.connector = connector;
        this.partitions = partitions;
        this.decoder = decoder;
        this.processor = processor;
        this.executor = executor;
        this.initialDelay = initialDelay;
        this.maxDelay = maxDelay;
        this.retryResetDelay = retryResetDelay;
        this.maxRetries = maxRetries;
        this.shutdownOnFatal = shutdownOnFatal;
    }

    /**
     *
     * @param server a reference to the Jetty Server.
     */
    public void setServer(LifeCycle server)
    {
        this.server = server;
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
        final Set<Map.Entry<String, List<KafkaMessageStream<T>>>> streams =
                connector.createMessageStreams(partitions, decoder).entrySet();

        for (final Map.Entry<String, List<KafkaMessageStream<T>>> e : streams) {
            final String topic = e.getKey();
            final List<KafkaMessageStream<T>> messageStreams = e.getValue();

            LOG.info("Consuming from topic '{}' with {} threads", topic, messageStreams.size());

            for (final KafkaMessageStream<T> stream : messageStreams) {
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
     * Shuts down the Jetty Server
     */
    private void shutDownServer()
    {
        if(this.server != null){
            Thread shutDownThread = new Thread(){
              public void run() {
                  try {
                      server.stop();
                  } catch (Exception e) {
                      LOG.error("Error occurred while attempting to shut down Jetty.");
                  }
              }
            };
            shutDownThread.setName("Unrecoverable_Error_Jetty_Shutdown_Thread");
            shutDownThread.setDaemon(true);
            shutDownThread.start();
        }
    }

    /**
     * Determines if this {@link KafkaConsumer} is currently consuming.
     *
     * @return true if this {@link KafkaConsumer} is currently consuming from at least one
     *         partition and no fatal errors have been detected; otherwise, false.
     */
    public boolean isRunning() {
        return !executor.isShutdown() && !executor.isTerminated() && !fatalErrorOccurred;
    }

    /**
     * Captures the fact that one of the {@link StreamProcessorRunnable} instances has terminated unexpectedly
     *
     * This is exposed via the {@link #isRunning() isRunning} method.
     */
    private void fatalErrorInStreamProcessor() {
        this.fatalErrorOccurred = true;
    }

    /**
     * A {@link Runnable} that processes a {@link KafkaMessageStream}.
     *
     * The configured {@link StreamProcessor} is used to process the stream.
     */
    private class StreamProcessorRunnable implements Runnable {

        private final KafkaMessageStream<T> stream;
        private final String topic;
        private int exponent = 0;
        private int retryCount = 0;
        private long timeStampOfLastRecoverableError = 0;

        /**
         * Creates a {@link StreamProcessorRunnable} for the given topic and stream.
         *
         * @param topic the topic the {@link KafkaMessageStream} belongs to.
         * @param stream a stream of {@link kafka.message.Message}s in the topic.
         */
        public StreamProcessorRunnable(final String topic, final KafkaMessageStream<T> stream) {
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
                LOG.warn("Fatal Error processing stream, stopping consumer", e);
                error(e);
            } catch (final Exception e) {
                recoverableError(e);
            } catch (final Throwable e) {
                LOG.warn("Fatal Error processing stream, stopping consumer", e);
                error(e);
            }
        }

        private void recoverableError(final Exception e) {
            LOG.warn("Error processing stream, restarting stream consumer", e);
            //Check if the time between recoverable errors is large enough to reset the retry count and sleepTime
            final long now = System.currentTimeMillis();
            if (now - timeStampOfLastRecoverableError >= retryResetDelay.toMilliseconds()) {
                retryCount = 0;
                exponent = 0;
            }
            //Determine how long to sleep for
            long sleepTime = (long)(initialDelay.toMilliseconds() * Math.pow( 2, ++exponent));
            if (sleepTime > maxDelay.toMilliseconds()) {
                sleepTime = maxDelay.toMilliseconds();
                exponent = 0;
            }
            //If a ceiling has been set on the number of retries, check if we have reached the ceiling
            retryCount++;
            if (maxRetries > -1 && retryCount >= maxRetries) {
                LOG.warn("Maximum number of retries reached ("+maxRetries+"), transitioning to unrecoverable error state.");
                error(e);
            } else {
                try {
                    Thread.sleep(sleepTime);
                } catch(InterruptedException ie){
                    LOG.warn("Error recovery grace period interrupted.", ie);
                }
                timeStampOfLastRecoverableError = System.currentTimeMillis();
                executor.execute(this);
            }
        }

        private void error(final Throwable e) {
            LOG.error("Unrecoverable error processing stream, shutting down.", e);
            fatalErrorInStreamProcessor();
            try {
                stop();
            } catch (final Exception ex) {
                throw new RuntimeException(ex);
            } finally {
                if (shutdownOnFatal) {
                  shutDownServer();
                }
            }
        }
    }
}
