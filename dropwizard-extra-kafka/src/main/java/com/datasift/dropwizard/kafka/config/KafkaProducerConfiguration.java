package com.datasift.dropwizard.kafka.config;

import com.datasift.dropwizard.kafka.util.Compression;
import com.yammer.dropwizard.util.Duration;
import com.yammer.dropwizard.util.Size;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * Configuration for the Kafka producer.
 * <p>
 * By default, the producer will be synchronous, blocking the calling thread
 * until the message has been sent.
 * <p>
 * To use an asynchronous producer, set {@link KafkaProducerConfiguration#async}
 * with the desired properties.
 */
public class KafkaProducerConfiguration extends KafkaClientConfiguration {

    /**
     * Size of the client-side send buffer.
     */
    @JsonProperty
    @NotNull
    protected Size sendBufferSize = Size.kilobytes(10);

    /**
     * Maximum time to wait on connection to a broker.
     */
    @JsonProperty
    @NotNull
    protected Duration connectionTimeout = Duration.milliseconds(5000);

    /**
     * Number of produce requests to a broker after which to reset the connection.
     */
    @JsonProperty
    @Min(0)
    protected long reconnectInterval = 30000;

    /**
     * Maximum size of a message payload.
     * <p>
     * Attempts to produce a {@link kafka.message.Message} with a payload that
     * exceeds this limit will cause the producer to throw a
     * {@link kafka.common.MessageSizeTooLargeException}.
     */
    @JsonProperty
    @NotNull
    protected Size maxMessageSize = Size.megabytes(1);

    /**
     * Compression codec for compressing all messages.
     *
     * @see KafkaProducerConfiguration#getCompression()
     */
    @JsonProperty
    protected Compression compression = Compression.parse("none");

    /**
     * Optional list of the topics to compress.
     * <p>
     * If {@link KafkaProducerConfiguration#compression} is enabled, this
     * filters the topics compression is enabled for. Leaving this empty and
     * enabling compression will cause all topics to be compressed.
     */
    @JsonProperty
    protected String[] compressedTopics = new String[0];

    /**
     * Number of retries for refreshing the partition cache after a cache miss.
     */
    @JsonProperty
    @Min(0)
    protected int partitionMissRetries = 3;

    /**
     * Configuration for the asynchronous producer, defaults to synchronous.
     * <p>
     * If this is provided, the producer will be asynchronous; otherwise, it
     * will be synchronous.
     *
     * @see KafkaAsyncProducerConfiguration
     */
    @JsonProperty
    protected KafkaAsyncProducerConfiguration async = null;

    /**
     * @see KafkaProducerConfiguration#sendBufferSize
     */
    public Size getSendBufferSize() {
        return sendBufferSize;
    }

    /**
     * @see KafkaProducerConfiguration#connectionTimeout
     */
    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    /**
     * @see KafkaProducerConfiguration#reconnectInterval
     */
    public long getReconnectInterval() {
        return reconnectInterval;
    }

    /**
     * @see KafkaProducerConfiguration#maxMessageSize
     */
    public Size getMaxMessageSize() {
        return maxMessageSize;
    }

    /**
     * @throws IllegalArgumentException if the compression codec is invalid or
     *                                  unsupported
     * @see KafkaProducerConfiguration#compression
     */
    public Compression getCompression() {
        return compression == null ? Compression.parse("default") : compression;
    }

    /**
     * @see KafkaProducerConfiguration#compressedTopics
     */
    public String[] getCompressedTopics() {
        return compressedTopics == null ? new String[0] : compressedTopics;
    }

    /**
     * @see KafkaProducerConfiguration#partitionMissRetries
     */
    public int getPartitionMissRetries() {
        return partitionMissRetries;
    }

    /**
     * @see KafkaProducerConfiguration#async
     */
    public KafkaAsyncProducerConfiguration getAsync() {
        return async;
    }

    /**
     * Determines whether the configured producer should be asynchronous.
     *
     * @return true if the producer should be asynchronous; otherwise, false.
     */
    public boolean isAsync() {
        return async != null;
    }
}
