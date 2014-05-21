package com.datasift.dropwizard.kafka;

import io.dropwizard.util.Duration;
import io.dropwizard.util.Size;
import com.datasift.dropwizard.kafka.consumer.KafkaConsumer;
import com.datasift.dropwizard.kafka.consumer.KafkaConsumerHealthCheck;
import com.datasift.dropwizard.kafka.consumer.StreamProcessor;
import com.datasift.dropwizard.kafka.consumer.SynchronousConsumer;
import io.dropwizard.setup.Environment;
import com.datasift.dropwizard.zookeeper.ZooKeeperFactory;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.message.Message;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import kafka.utils.VerifiableProperties;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

/**
 * A factory for creating and managing {@link KafkaConsumer} instances.
 * <p/>
 * The {@link KafkaConsumer} implementation will be determined by the configuration used to create
 * it.
 * <p/>
 * The resultant {@link KafkaConsumer} will have its lifecycle managed by the {@link Environment}
 * and will have {@link com.codahale.metrics.health.HealthCheck}s installed to monitor its status.
 */
public class KafkaConsumerFactory extends KafkaClientFactory {

    private static final Decoder<byte[]> DefaultDecoder = new DefaultDecoder(new VerifiableProperties());

    /**
     * A description of the initial offset to consume from a partition when no committed offset
     * exists.
     * <p/>
     * <dl>
     *     <dt>SMALLEST</dt><dd>Use the smallest (i.e. earliest) available offset. In effect,
     *                          consuming the entire log.</dd>
     *     <dt>LARGEST</dt><dd>Use the largest (i.e. latest) available offset. In effect,
     *                         tailing the end of the log.</dd>
     * </dl>
     */
    public enum InitialOffset { SMALLEST, LARGEST, ERROR }

    @NotEmpty
    protected String group = "";

    @NotNull
    protected Map<String, Integer> partitions = ImmutableMap.of();

    protected Duration timeout = null;

    @NotNull
    protected Size receiveBufferSize = Size.kilobytes(64);

    @NotNull
    protected Size fetchSize = Size.kilobytes(300);

    @NotNull
    protected Duration backOffIncrement = Duration.seconds(1);

    @Min(0)
    protected int queuedChunks = 100;

    protected boolean autoCommit = true;

    @NotNull
    protected Duration autoCommitInterval = Duration.seconds(10);

    @NotNull
    protected InitialOffset initialOffset = InitialOffset.LARGEST;

    @Min(0)
    protected int rebalanceRetries = 4;

    @NotNull
    protected Duration initialRecoveryDelay = Duration.milliseconds(500);

    @NotNull
    protected Duration maxRecoveryDelay = Duration.minutes(5);

    @NotNull
    protected Duration retryResetDelay = Duration.minutes(2);

    @Min(-1)
    protected int maxRecoveryAttempts = 20;

    @NotNull
    protected boolean shutdownOnFatal = false;

    @NotNull
    protected Duration shutdownGracePeriod = Duration.seconds(5);

    /**
     * Returns the consumer group the {@link KafkaConsumer} belongs to.
     *
     * @return the consumer group the {@link KafkaConsumer} belongs to.
     */
    @JsonProperty
    public String getGroup() {
        return group;
    }

    /**
     * Sets the consumer group the {@link KafkaConsumer} belongs to.
     *
     * @param group the consumer group the {@link KafkaConsumer} belongs to.
     */
    @JsonProperty
    public void setGroup(final String group) {
        this.group = group;
    }

    /**
     * Returns a mapping of the number of partitions to consume from each topic.
     * <p/>
     * Topics not referenced will not be consumed from.
     *
     * @return a Map of topics to the number of partitions to consume from them.
     */
    @JsonProperty
    public Map<String, Integer> getPartitions() {
        return partitions;
    }

    /**
     * Sets a mapping of the number of partitions to consume from each topic.
     * <p/>
     * Topics not referenced will not be consumed from.
     *
     * @param partitions a Map of topics to the number of partitions to consume from them.
     */
    @JsonProperty
    public void getPartitions(final Map<String, Integer> partitions) {
        this.partitions = partitions;
    }

    /**
     * Returns the time the {@link KafkaConsumer} should wait to receive messages before timing out
     * the stream.
     * <p/>
     * When a {@link KafkaConsumer} times out a stream, a {@link
     * kafka.consumer.ConsumerTimeoutException} will be thrown by that streams' {@link
     * kafka.consumer.ConsumerIterator}.
     *
     * @return the maximum time to wait when receiving messages from a broker before timing out.
     *
     * @see kafka.consumer.ConsumerIterator#next()
     */
    @JsonProperty
    public Duration getTimeout() {
        return timeout == null
                ? Duration.milliseconds(-1)
                : timeout;
    }

    /**
     * Sets the time the {@link KafkaConsumer} should wait to receive messages before timing out
     * the stream.
     * <p/>
     * When a {@link KafkaConsumer} times out a stream, a {@link
     * kafka.consumer.ConsumerTimeoutException} will be thrown by that streams' {@link
     * kafka.consumer.ConsumerIterator}.
     *
     * @param timeout the maximum time to wait when receiving messages before timing out.
     *
     * @see kafka.consumer.ConsumerIterator#next()
     */
    @JsonProperty
    public void setTimeout(final Duration timeout) {
        this.timeout = timeout;
    }

    /**
     * Returns the size of the client-side receive buffer.
     *
     * @return the size of the client-side receive buffer.
     */
    @JsonProperty
    public Size getReceiveBufferSize() {
        return receiveBufferSize;
    }

    /**
     * Sets the size of the client-side receive buffer.
     *
     * @param size the size of the client-side receive buffer.
     */
    @JsonProperty
    public void getReceiveBufferSize(final Size size) {
        this.receiveBufferSize = size;
    }

    /**
     * Returns the maximum size of a batch of messages to fetch in a single request.
     * <p/>
     * This dictates the maximum size of a message that may be received by the {@link
     * KafkaConsumer}. Messages larger than this size will cause a {@link
     * kafka.common.InvalidMessageSizeException} to be thrown during iteration of the stream.
     *
     * @return the maximum size of a batch of messages to receive in a single request.
     *
     * @see kafka.javaapi.message.ByteBufferMessageSet#iterator()
     */
    @JsonProperty
    public Size getFetchSize() {
        return fetchSize;
    }

    /**
     * Sets the maximum size of a batch of messages to fetch in a single request.
     * <p/>
     * This dictates the maximum size of a message that may be received by the {@link
     * KafkaConsumer}. Messages larger than this size will cause a {@link
     * kafka.common.InvalidMessageSizeException} to be thrown during iteration of the stream.
     *
     * @param size the maximum size of a batch of messages to receive in a single request.
     *
     * @see kafka.javaapi.message.ByteBufferMessageSet#iterator()
     */
    @JsonProperty
    public void getFetchSize(final Size size) {
        this.fetchSize = size;
    }

    /**
     * Returns the cumulative delay before polling a broker again when no data is returned.
     * <p/>
     * When fetching data from a broker, if there is no new data, there will be a delay before
     * polling the broker again. This controls the duration of the delay by increasing it linearly,
     * on each poll attempt.
     *
     * @return the amount by which the retry timeout will be increased after each attempt.
     */
    @JsonProperty
    public Duration getBackOffIncrement() {
        return backOffIncrement;
    }

    /**
     * Sets the cumulative delay before polling a broker again when no data is returned.
     * <p/>
     * When fetching data from a broker, if there is no new data, there will be a delay before
     * polling the broker again. This controls the duration of the delay by increasing it linearly,
     * on each poll attempt.
     *
     * @param increment the amount by which the retry timeout will be increased after each attempt.
     */
    @JsonProperty
    public void getBackOffIncrement(final Duration increment) {
        this.backOffIncrement = increment;
    }

    /**
     * Returns the maximum number of chunks to queue in internal buffers.
     * <p/>
     * The consumer internally buffers fetched messages in a set of queues, which are used to
     * iterate the stream. This controls the size of these queues.
     * <p/>
     * Once a queue has been filled, it will block subsequent attempts to fill it until (some of) it
     * has been iterated.
     */
    @JsonProperty
    public int getQueuedChunks() {
        return queuedChunks;
    }

    /**
     * Sets the maximum number of chunks to queue in internal buffers.
     * <p/>
     * The consumer internally buffers fetched messages in a set of queues, which are used to
     * iterate the stream. This controls the size of these queues.
     * <p/>
     * Once a queue has been filled, it will block subsequent attempts to fill it until (some of) it
     * has been iterated.
     */
    @JsonProperty
    public void getQueuedChunks(final int maxChunks) {
        this.queuedChunks = maxChunks;
    }

    /**
     * Returns whether to automatically commit the offsets that have been consumed.
     *
     * @return true to commit the last consumed offset periodically; false to never commit offsets.
     *
     * @see #getAutoCommitInterval
     */
    @JsonProperty
    public boolean getAutoCommit() {
        return autoCommit;
    }

    /**
     * Sets whether to automatically commit the offsets that have been consumed.
     *
     * @param autoCommit true to commit the last consumed offset periodically;
     *                   false to never commit offsets.
     *
     * @see #getAutoCommitInterval
     */
    @JsonProperty
    public void setAutoCommit(final boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    /**
     * Sets the frequency to automatically commit previously consumed offsets, if enabled.
     *
     * @return the frequency to automatically commit the previously consumed offsets, when enabled.
     *
     * @see #getAutoCommit
     */
    @JsonProperty
    public Duration getAutoCommitInterval() {
        return autoCommitInterval;
    }


    /**
     * Returns the frequency to automatically commit previously consumed offsets, if enabled.
     *
     * @return the frequency to automatically commit the previously consumed offsets, when enabled.
     *
     * @see #getAutoCommit
     */
    @JsonProperty
    public void getAutoCommitInterval(final Duration autoCommitInterval) {
        this.autoCommitInterval = autoCommitInterval;
    }

    /**
     * Returns the setting for the initial offset to consume from when no committed offset exists.
     *
     * @return the initial offset to consume from in a partition.
     *
     * @see InitialOffset
     */
    @JsonProperty
    public InitialOffset getInitialOffset() {
        return initialOffset;
    }

    /**
     * Sets the setting for the initial offset to consume from when no committed offset exists.
     *
     * @param initialOffset the initial offset to consume from in a partition.
     *
     * @see InitialOffset
     */
    @JsonProperty
    public void setInitialOffset(final InitialOffset initialOffset) {
        this.initialOffset = initialOffset;
    }

    /**
     * Returns the maximum number of retries during a re-balance.
     *
     * @return the maximum number of times to retry a re-balance operation.
     */
    @JsonProperty
    public int getRebalanceRetries() {
        return rebalanceRetries;
    }

    /**
     * Sets the maximum number of retries during a re-balance.
     *
     * @param rebalanceRetries the maximum number of times to retry a re-balance operation.
     */
    @JsonProperty
    public void getRebalanceRetries(final int rebalanceRetries) {
        this.rebalanceRetries = rebalanceRetries;
    }

    public Duration getInitialRecoveryDelay() {
        return initialRecoveryDelay;
    }

    public void setInitialRecoveryDelay(final Duration initialRecoveryDelay) {
        this.initialRecoveryDelay = initialRecoveryDelay;
    }

    public Duration getMaxRecoveryDelay() {
        return maxRecoveryDelay;
    }

    public void setMaxRecoveryDelay(final Duration maxRecoveryDelay) {
        this.maxRecoveryDelay = maxRecoveryDelay;
    }

    public Duration getRetryResetDelay() {
        return retryResetDelay;
    }

    public void setRetryResetDelay(final Duration retryResetDelay) {
        this.retryResetDelay = retryResetDelay;
    }

    public int getMaxRecoveryAttempts() {
        return maxRecoveryAttempts;
    }

    public void setMaxRecoveryAttempts(final int maxRecoveryAttempts) {
        this.maxRecoveryAttempts = maxRecoveryAttempts;
    }

    public boolean isShutdownOnFatal() {
        return shutdownOnFatal;
    }

    public void setShutdownOnFatal(final boolean shutdownOnFatal) {
        this.shutdownOnFatal = shutdownOnFatal;
    }

    public Duration getShutdownGracePeriod() {
        return shutdownGracePeriod;
    }

    public void setShutdownGracePeriod(final Duration shutdownGracePeriod) {
        this.shutdownGracePeriod = shutdownGracePeriod;
    }

    /**
     * Prepares a {@link KafkaConsumerBuilder} for a given {@link StreamProcessor}.
     *
     * @param processor the {@link StreamProcessor} to process the stream with.
     * @return a {@link KafkaConsumerBuilder} to build a {@link KafkaConsumer} for the given
     *         processor.
     */
    public KafkaConsumerBuilder<byte[], byte[]> processWith(final StreamProcessor<byte[], byte[]> processor) {
        return processWith(DefaultDecoder, processor);
    }

    /**
     * Prepares a {@link KafkaConsumerBuilder} for a given {@link Decoder} and {@link
     * StreamProcessor}.
     * <p/>
     * The decoder instance is used to decode {@link Message}s in the stream before being passed to
     * the processor.
     *
     * @param decoder the {@link Decoder} instance to decode messages with
     * @param processor a {@link StreamProcessor} to process the message stream
     * @return a {@link KafkaConsumerBuilder} to build a {@link KafkaConsumer} for the given
     *         processor and decoder.
     */
    public <V> KafkaConsumerBuilder<byte[], V> processWith(final Decoder<V> decoder,
                                                           final StreamProcessor<byte[], V> processor) {
        return new KafkaConsumerBuilder<>(DefaultDecoder, decoder, processor);
    }

    public <K, V> KafkaConsumerBuilder<K, V> processWith(final Decoder<K> keyDecoder,
                                                         final Decoder<V> valueDecoder,
                                                         final StreamProcessor<K, V> processor) {
        return new KafkaConsumerBuilder<>(keyDecoder, valueDecoder, processor);
    }

    /**
     * A Builder for building a configured {@link KafkaConsumer}.
     *
     * @param <V> the type of the messages the {@link KafkaConsumer} will process.
     */
    public class KafkaConsumerBuilder<K, V> {

        private final Decoder<K> keyDecoder;
        private final Decoder<V> valueDecoder;
        private final StreamProcessor<K, V> processor;
        private static final String DEFAULT_NAME = "kafka-consumer-default";

        private KafkaConsumerBuilder(final Decoder<K> keyDecoder,
                                     final Decoder<V> valueDecoder,
                                     final StreamProcessor<K, V> processor) {
            this.keyDecoder = keyDecoder;
            this.valueDecoder = valueDecoder;
            this.processor = processor;
        }

        /**
         * Builds a {@link KafkaConsumer} instance for the given {@link Environment}.
         *
         * @param environment the {@link Environment} to build {@link KafkaConsumer} instances for.
         *
         * @return a managed and configured {@link KafkaConsumer}.
         */
        public KafkaConsumer build(final Environment environment) {
            return build(environment, DEFAULT_NAME);
        }

        /**
         * Builds a {@link KafkaConsumer} instance from the given {@link ExecutorService} and name,
         * for the given {@link Environment}.
         * <p/>
         * The name is used to identify the returned {@link KafkaConsumer} instance, for example, as
         * the name of its {@link com.codahale.metrics.health.HealthCheck}s, thread pool, etc.
         * <p/>
         * This implementation creates a new {@link ExecutorService} with a fixed-size thread-pool,
         * configured for one thread per-partition the {@link KafkaConsumer} is being configured to
         * consume.
         *
         * @param environment the {@link Environment} to build {@link KafkaConsumer} instances for.
         * @param name the name of the {@link KafkaConsumer}.
         *
         * @return a managed and configured {@link KafkaConsumer}.
         */
        public KafkaConsumer build(final Environment environment, final String name) {

            int threads = 0;
            for (final Integer p : getPartitions().values()) {
                threads = threads + p;
            }

            final ExecutorService executor = environment.lifecycle()
                    .executorService(name + "-%d")
                        .minThreads(threads)
                        .maxThreads(threads)
                        .keepAliveTime(Duration.seconds(0))
                        .build();

            return build(environment, executor, name);
        }

        /**
         * Builds a {@link KafkaConsumer} instance from the given {@link ExecutorService} and name,
         * for the given {@link Environment}.
         * <p/>
         * The name is used to identify the returned {@link KafkaConsumer} instance, for example, as
         * the name of its {@link com.codahale.metrics.health.HealthCheck}s, etc.
         *
         * @param environment the {@link Environment} to build {@link KafkaConsumer} instances for.
         * @param executor the {@link ExecutorService} to process messages with.
         * @param name the name of the {@link KafkaConsumer}.
         *
         * @return a managed and configured {@link KafkaConsumer}.
         */
        public KafkaConsumer build(final Environment environment,
                                   final ExecutorService executor,
                                   final String name) {

            final SynchronousConsumer consumer = new SynchronousConsumer<>(
                    Consumer.createJavaConsumerConnector(toConsumerConfig(KafkaConsumerFactory.this)),
                    getPartitions(),
                    keyDecoder,
                    valueDecoder,
                    processor,
                    executor,
                    getInitialRecoveryDelay(),
                    getMaxRecoveryDelay(),
                    getRetryResetDelay(),
                    getMaxRecoveryAttempts(),
                    isShutdownOnFatal(),
                    getShutdownGracePeriod());

            // manage the consumer
            environment.lifecycle().manage(consumer);

            // add health checks
            environment.healthChecks().register(name, new KafkaConsumerHealthCheck(consumer));

            return consumer;
        }
    }

    static ConsumerConfig toConsumerConfig(final KafkaConsumerFactory factory) {
        final ZooKeeperFactory zookeeper = factory.getZookeeper();
        final Properties props = new Properties();

        props.setProperty("zookeeper.connect",
                zookeeper.getQuorumSpec());
        props.setProperty("zookeeper.connection.timeout.ms",
                String.valueOf(zookeeper.getConnectionTimeout().toMilliseconds()));
        props.setProperty("zookeeper.session.timeout.ms",
                String.valueOf(zookeeper.getSessionTimeout().toMilliseconds()));
        props.setProperty("group.id",
                factory.getGroup());
        props.setProperty("socket.timeout.ms",
                String.valueOf(factory.getSocketTimeout().toMilliseconds()));
        props.setProperty("socket.receive.buffer.bytes",
                String.valueOf(factory.getReceiveBufferSize().toBytes()));
        props.setProperty("fetch.message.max.bytes",
                String.valueOf(factory.getFetchSize().toBytes()));
        props.setProperty("fetch.wait.max.ms",
                String.valueOf(factory.getBackOffIncrement().toMilliseconds()));
        props.setProperty("queued.max.message.chunks",
                String.valueOf(factory.getQueuedChunks()));
        props.setProperty("auto.commit.enable",
                String.valueOf(factory.getAutoCommit()));
        props.setProperty("auto.commit.interval.ms",
                String.valueOf(factory.getAutoCommitInterval().toMilliseconds()));
        props.setProperty("auto.offset.reset",
                String.valueOf(factory.getInitialOffset()).toLowerCase());
        props.setProperty("consumer.timeout.ms",
                String.valueOf(factory.getTimeout().toMilliseconds()));
        props.setProperty("rebalance.max.retries",
                String.valueOf(factory.getRebalanceRetries()));

        return new ConsumerConfig(props);
    }
}
