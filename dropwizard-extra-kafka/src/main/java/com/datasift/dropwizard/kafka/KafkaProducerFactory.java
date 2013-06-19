package com.datasift.dropwizard.kafka;

import com.datasift.dropwizard.kafka.KafkaClientFactory;
import com.datasift.dropwizard.kafka.util.Compression;
import com.codahale.dropwizard.util.Duration;
import com.codahale.dropwizard.util.Size;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * Configuration for the Kafka producer.
 * <p/>
 * By default, the producer will be synchronous, blocking the calling thread until the message has
 * been sent.
 * <p/>
 * To use an asynchronous producer, set {@link KafkaProducerFactory#async} with the desired
 * properties.
 */
public class KafkaProducerFactory extends KafkaClientFactory {

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
     * <p/>
     * Attempts to produce a {@link kafka.message.Message} with a payload that exceeds this limit
     * will cause the producer to throw a {@link kafka.common.MessageSizeTooLargeException}.
     */
    @JsonProperty
    @NotNull
    protected Size maxMessageSize = Size.megabytes(1);

    /**
     * Compression codec for compressing all messages.
     *
     * @see KafkaProducerFactory#getCompression()
     */
    @JsonProperty
    protected Compression compression = Compression.parse("none");

    /**
     * Optional list of the topics to compress.
     * <p/>
     * If {@link KafkaProducerFactory#compression} is enabled, this filters the topics
     * compression is enabled for. Leaving this empty and enabling compression will cause all topics
     * to be compressed.
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
     * <p/>
     * If this is provided, the producer will be asynchronous; otherwise, it will be synchronous.
     *
     * @see KafkaAsyncProducerFactory
     */
    @JsonProperty
    protected KafkaAsyncProducerFactory async = null;

    /**
     * @see KafkaProducerFactory#sendBufferSize
     */
    public Size getSendBufferSize() {
        return sendBufferSize;
    }

    /**
     * @see KafkaProducerFactory#connectionTimeout
     */
    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    /**
     * @see KafkaProducerFactory#reconnectInterval
     */
    public long getReconnectInterval() {
        return reconnectInterval;
    }

    /**
     * @see KafkaProducerFactory#maxMessageSize
     */
    public Size getMaxMessageSize() {
        return maxMessageSize;
    }

    /**
     * @throws IllegalArgumentException if the compression codec is invalid or
     *                                  unsupported
     * @see KafkaProducerFactory#compression
     */
    public Compression getCompression() {
        return compression == null
                ? Compression.parse("default")
                : compression;
    }

    /**
     * @see KafkaProducerFactory#compressedTopics
     */
    public String[] getCompressedTopics() {
        return compressedTopics == null
                ? new String[0]
                : compressedTopics;
    }

    /**
     * @see KafkaProducerFactory#partitionMissRetries
     */
    public int getPartitionMissRetries() {
        return partitionMissRetries;
    }

    /**
     * @see KafkaProducerFactory#async
     */
    public KafkaAsyncProducerFactory getAsync() {
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
