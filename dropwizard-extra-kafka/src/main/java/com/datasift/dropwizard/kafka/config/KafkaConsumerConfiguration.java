package com.datasift.dropwizard.kafka.config;

import com.google.common.collect.ImmutableMap;
import com.yammer.dropwizard.util.Duration;
import com.yammer.dropwizard.util.Size;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;
import com.datasift.dropwizard.kafka.consumer.KafkaConsumer;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.Map;

/**
 * Configuration for a {@link KafkaConsumer}.
 */
public class KafkaConsumerConfiguration extends KafkaClientConfiguration {

    /**
     * Consumer group the {@link KafkaConsumer} belongs to.
     */
    @JsonProperty
    @NotEmpty
    protected String group = "";

    /**
     * Mapping of the number of partitions to consume from each topic.
     * <p/>
     * Topics not referenced will not be consumed from.
     */
    @JsonProperty
    @NotNull
    protected Map<String, Integer> partitions = ImmutableMap.of();

    /**
     * Time the {@link KafkaConsumer} should wait to receive messages before timing out the stream.
     * <p/>
     * When a {@link KafkaConsumer} times out a stream, a {@link
     * kafka.consumer.ConsumerTimeoutException} will be thrown by that streams' {@link
     * kafka.consumer.ConsumerIterator}.
     *
     * @see kafka.consumer.ConsumerIterator#next()
     */
    @JsonProperty
    protected Duration timeout = null;

    /**
     * Size of the client-side receive buffer.
     */
    @JsonProperty
    @NotNull
    protected Size receiveBufferSize = Size.kilobytes(64);

    /**
     * Maximum size of a batch of messages to fetch in a single request.
     * <p/>
     * This dictates the maximum size of a message that may be received by the {@link
     * KafkaConsumer}. Messages larger than this size will cause a {@link
     * kafka.common.InvalidMessageSizeException} to be thrown during iteration of the stream.
     *
     * @see kafka.javaapi.message.ByteBufferMessageSet#iterator()
     */
    @JsonProperty
    @NotNull
    protected Size fetchSize = Size.kilobytes(300);

    /**
     * Cumulative delay before polling a broker again when no data is returned.
     * <p/>
     * When fetching data from a broker, if there is no new data, there will be a delay before
     * polling the broker again. This controls the duration of the delay by increasing it linearly,
     * on each poll attempt.
     */
    @JsonProperty
    @NotNull
    protected Duration backOffIncrement = Duration.seconds(1);

    /**
     * Maximum number of chunks to queue in internal buffers.
     * <p/>
     * The consumer internally buffers fetched messages in a set of queues, which are used to
     * iterate the stream. This controls the size of these queues.
     * <p/>
     * Once a queue has been filled, it will block subsequent attempts to fill it until (some of) it
     * has been iterated.
     */
    @JsonProperty
    @Min(0)
    protected int queuedChunks = 100;

    /**
     * Automatically commits the currently consumed offsets periodically.
     *
     * @see KafkaConsumerConfiguration#autoCommitInterval
     */
    @JsonProperty
    protected boolean autoCommit = true;

    /**
     * Frequency to automatically commit currently consumed offsets, if enabled.
     *
     * @see KafkaConsumerConfiguration#autoCommitInterval
     */
    @JsonProperty
    @NotNull
    protected Duration autoCommitInterval = Duration.seconds(10);

    /**
     * Maximum number of retries during a re-balance.
     */
    @JsonProperty
    @Min(0)
    protected int rebalanceRetries = 4;

    /**
     * Initial delay before attempting recovery of a failed consumer.
     */
    @JsonProperty
    @NotNull
    protected Duration initialRecoveryDelay = Duration.milliseconds(500);


    /**
     * The maximum delay between recovery attempts of a KafkaConsumer.
     */
    @JsonProperty
    @NotNull
    protected Duration maxRecoveryDelay = Duration.minutes(5);

    /**
     * If no errors have occurred for this duration, the KafkaConsumer is assumed to have returned to normal conditions.
     * The retry count is reverted to zero and the delay between retries is reset to initialDelay
     */
    @JsonProperty
    @NotNull
    protected Duration retryResetDelay = Duration.minutes(2);

    /**
     * Maximum number of continuous recovery attempts before moving to an unrecoverable state.
     * <p/>
     * -1 indicates no upper limit to the number of retries.
     */
    @JsonProperty
    @Min(-1)
    protected int maxRecoveryAttempts = 20;

    /**
     * Whether to gracefully shutdown the server in the event of an unrecoverable error.
     */
    @JsonProperty
    @NotNull
    protected boolean shutdownOnFatal = false;

    @JsonProperty
    @NotNull
    protected Duration shutdownGracePeriod = Duration.seconds(5);

    /**
     * @see KafkaConsumerConfiguration#group
     */
    public String getGroup() {
        return group;
    }

    /**
     * @see KafkaConsumerConfiguration#partitions
     */
    public Map<String, Integer> getPartitions() {
        return partitions;
    }

    /**
     * @see KafkaConsumerConfiguration#timeout
     */
    public Duration getTimeout() {
        return timeout == null
                ? Duration.milliseconds(-1)
                : timeout;
    }

    /**
     * @see KafkaConsumerConfiguration#receiveBufferSize
     */
    public Size getReceiveBufferSize() {
        return receiveBufferSize;
    }

    /**
     * @see KafkaConsumerConfiguration#fetchSize
     */
    public Size getFetchSize() {
        return fetchSize;
    }

    /**
     * @see KafkaConsumerConfiguration#backOffIncrement
     */
    public Duration getBackOffIncrement() {
        return backOffIncrement;
    }

    /**
     * @see KafkaConsumerConfiguration#queuedChunks
     */
    public int getQueuedChunks() {
        return queuedChunks;
    }

    /**
     * @see KafkaConsumerConfiguration#autoCommit
     */
    public boolean getAutoCommit() {
        return autoCommit;
    }

    /**
     * @see KafkaConsumerConfiguration#autoCommitInterval
     */
    public Duration getAutoCommitInterval() {
        return autoCommitInterval;
    }

    /**
     * @see KafkaConsumerConfiguration#rebalanceRetries
     */
    public int getRebalanceRetries() {
        return rebalanceRetries;
    }

    /**
     * @see KafkaConsumerConfiguration#initialRecoveryDelay
     */
    public Duration getInitialRecoveryDelay() {
        return initialRecoveryDelay == null
                ? Duration.milliseconds(500)
                : initialRecoveryDelay;
    }

    /**
     * @see KafkaConsumerConfiguration#maxRecoveryDelay
     */
    public Duration getMaxRecoveryDelay() {
        return maxRecoveryDelay == null
                ? Duration.minutes(5)
                : maxRecoveryDelay;
    }

    /**
     * @see KafkaConsumerConfiguration#retryResetDelay
     */
    public Duration getRetryResetDelay() {
        return retryResetDelay == null
                ? Duration.minutes(2)
                : retryResetDelay;
    }

    /**
     * @see KafkaConsumerConfiguration#maxRecoveryAttempts
     */
    public int getMaxRecoveryAttempts() {
        return maxRecoveryAttempts;
    }

    /**
     * @see KafkaConsumerConfiguration#shutdownOnFatal
     */
    public boolean isShutdownOnFatal() {
        return shutdownOnFatal;
    }

    public Duration getShutdownGracePeriod() {
        return shutdownGracePeriod;
    }
}
