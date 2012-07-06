package com.datasift.dropwizard.kafka.consumer;

import com.datasift.dropwizard.kafka.config.ErrorPolicy;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yammer.dropwizard.logging.Log;
import com.yammer.dropwizard.util.Duration;
import kafka.consumer.KafkaMessageStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import kafka.serializer.Decoder;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * A {@link KafkaConsumer} that uses a fixed-sized {@link java.util.concurrent.ThreadPoolExecutor} to process messages.
 *
 * The number of threads used for the {@link java.util.concurrent.ThreadPoolExecutor}
 * is determined automatically as the total number of partitions this {@link KafkaConsumer}
 * is configured to consume.
 *
 * TODO: bit of a god object, break up in to smaller units?
 */
public class ThreadPooledConsumer implements KafkaConsumer {

    private Log LOG = Log.forClass(getClass());

    private ConsumerConnector connector;
    private Map<String, Integer> partitions;
    private ErrorPolicy errorPolicy;
    private ScheduledExecutorService executor;
    private Duration shutdownPeriod;

    public ThreadPooledConsumer(ConsumerConnector connector,
                                Map<String, Integer> partitions,
                                ErrorPolicy errorPolicy,
                                Duration shutdownPeriod,
                                String name) {
        this.connector = connector;
        this.partitions = partitions;
        this.errorPolicy = errorPolicy;
        this.shutdownPeriod = shutdownPeriod;

        // calculate number of threads needed for ThreadPool
        int threads = 0;
        for (Integer p : partitions.values()) {
            threads = threads + p;
        }

        // custom ThreadFactory to customize the Thread names
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat(String.format("kafka-consumer-%s-%%d", name))
                .build();

        this.executor = new ScheduledThreadPoolExecutor(threads, threadFactory);
    }

    public void consume(StreamProcessor<Message> processor) {
        processStreams(processor, connector.createMessageStreams(partitions));
    }

    public <T> void consume(StreamProcessor<T> processor, Decoder<T> decoder) {
        processStreams(processor, connector.createMessageStreams(partitions, decoder));
    }

    public void commitOffsets() {
        connector.commitOffsets();
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {
        executor.shutdownNow();
        executor.awaitTermination(shutdownPeriod.toNanoseconds(), TimeUnit.NANOSECONDS);
        connector.shutdown();
    }

    private <T> void processStreams(final StreamProcessor<T> processor,
                                    final Map<String, List<KafkaMessageStream<T>>> streams) {

        for (final Map.Entry<String, List<KafkaMessageStream<T>>> set : streams.entrySet()) {
            final String topic = set.getKey();
            final List<KafkaMessageStream<T>> messageStreams = set.getValue();

            LOG.info("Consuming from topic '{}' with {} threads",
                    messageStreams.size(), topic);

            for (final KafkaMessageStream<T> stream : set.getValue()) {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            processor.process(stream, topic);
                        } catch (Exception t) {
                            // only handle the error if the Thread hasn't been interrupted
                            if (!Thread.currentThread().isInterrupted()) {
                                onError(t, this);
                            }
                        }
                    }
                });
            }
        }
    }

    /**
     * Handle the specified error based on the configured {@link ErrorPolicy}
     *
     * If an error occurs while handling the error, the error is prompoted to a
     * fatal error regardless.
     *
     * @param e the {@link Exception} to handle
     * @param r the {@link Runnable} that triggered the error
     */
    private void onError(Exception e, Runnable r) {
        try {
            if (errorPolicy.getAction() == ErrorPolicy.ErrorAction.SHUTDOWN) {
                Thread.sleep(errorPolicy.getDelay().toMilliseconds());
                stop();
            } else {
                executor.schedule(r, errorPolicy.getDelay().toNanoseconds(), TimeUnit.NANOSECONDS);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
