package com.datasift.dropwizard.kafka;

import com.datasift.dropwizard.kafka.util.Compression;
import io.dropwizard.util.Duration;
import io.dropwizard.util.Size;
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

    @NotNull
    protected Size sendBufferSize = Size.kilobytes(10);

    @NotNull
    protected Duration connectionTimeout = Duration.milliseconds(5000);

    @Min(0)
    protected long reconnectInterval = 30000;

    @NotNull
    protected Size maxMessageSize = Size.megabytes(1);

    protected Compression compression = Compression.parse("none");

    protected String[] compressedTopics = new String[0];

    @Min(0)
    protected int partitionMissRetries = 3;

    protected KafkaAsyncProducerFactory async = null;

    /**
     * Returns the size of the client-side send buffer.
     *
     * @return the size of the client-side send buffer.
     */
    @JsonProperty
    public Size getSendBufferSize() {
        return sendBufferSize;
    }

    /**
     * Sets the size of the client-side send buffer.
     *
     * @param size the size of the client-side send buffer.
     */
    @JsonProperty
    public void setSendBufferSize(final Size size) {
        this.sendBufferSize = size;
    }

    /**
     * Returns the maximum time to wait on connection to a broker.
     *
     * @return the maximum time to wait on connection to a broker.
     */
    @JsonProperty
    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    /**
     * Sets the maximum time to wait on connection to a broker.
     *
     * @param timeout the maximum time to wait on connection to a broker.
     */
    @JsonProperty
    public void getConnectionTimeout(final Duration timeout) {
        this.connectionTimeout = timeout;
    }

    /**
     * Returns the number of produce requests to a broker after which to reset the connection.
     *
     * @return the number of produce requests to a broker after which the connection will be reset.
     */
    @JsonProperty
    public long getReconnectInterval() {
        return reconnectInterval;
    }

    /**
     * Sets the number of produce requests to a broker after which to reset the connection.
     *
     * @param interval the number of produce requests after which the connection will be reset.
     */
    @JsonProperty
    public void getReconnectInterval(final long interval) {
        this.reconnectInterval = interval;
    }

    /**
     * Returns the maximum size of a message payload.
     * <p/>
     * Attempts to produce a {@link kafka.message.Message} with a payload that exceeds this limit
     * will cause the producer to throw a {@link kafka.common.MessageSizeTooLargeException}.
     *
     * @return the maximum size of a message payload.
     */
    @JsonProperty
    public Size getMaxMessageSize() {
        return maxMessageSize;
    }

    /**
     * Sets the maximum size of a message payload.
     * <p/>
     * Attempts to produce a {@link kafka.message.Message} with a payload that exceeds this limit
     * will cause the producer to throw a {@link kafka.common.MessageSizeTooLargeException}.
     *
     * @param size the maximum size of a message payload.
     */
    @JsonProperty
    public void getMaxMessageSize(final Size size) {
        this.maxMessageSize = size;
    }

    /**
     * Returns the compression codec for compressing all messages of compressed topics.
     *
     * @return the compression codec for compressing messages with.
     * @throws IllegalArgumentException if the compression codec is invalid or unsupported.
     *
     * @see KafkaProducerFactory#getCompression()
     */
    @JsonProperty
    public Compression getCompression() {
        return compression == null
                ? Compression.parse("default")
                : compression;
    }
    /**
     * Sets the compression codec for compressing all messages of compressed topics.
     *
     * @param compression the compression codec for compressing messages with.
     * @throws IllegalArgumentException if the compression codec is invalid or unsupported.
     *
     * @see KafkaProducerFactory#getCompression()
     */
    @JsonProperty
    public void setCompression(final String compression) {
        this.compression = Compression.parse(compression);
    }

    /**
     * Returns an optional list of the topics to compress.
     * <p/>
     * If {@link #getCompression() compression} is enabled, this filters the topics compression is
     * enabled for. Leaving this empty and enabling compression will cause all topics to be
     * compressed.
     *
     * @return the list of topics compression is enabled for, or an empty list when compression is
     *         enabled for all topics.
     */
    @JsonProperty
    public String[] getCompressedTopics() {
        return compressedTopics == null
                ? new String[0]
                : compressedTopics;
    }

    /**
     * Returns an optional list of the topics to compress.
     * <p/>
     * If {@link #getCompression() compression} is enabled, this filters the topics compression is
     * enabled for. Leaving this empty and enabling compression will cause all topics to be
     * compressed.
     *
     * @param compressedTopics the list of topics compression is enabled for, or an empty list when
     *                         compression is enabled for all topics.
     */
    @JsonProperty
    public void setCompressedTopics(final String[] compressedTopics) {
        this.compressedTopics = compressedTopics;
    }

    /**
     * Returns the number of retries for refreshing the partition cache after a cache miss.
     *
     * @return the number of retries for refreshing the partition cache after a cache miss.
     */
    @JsonProperty
    public int getPartitionMissRetries() {
        return partitionMissRetries;
    }

    /**
     * Sets the number of retries for refreshing the partition cache after a cache miss.
     *
     * @param retries the number of retries for refreshing the partition cache after a cache miss.
     */
    @JsonProperty
    public void getPartitionMissRetries(final int retries) {
        this.partitionMissRetries = retries;
    }

    /**
     * Returns a factory for the asynchronous producer, defaults to synchronous.
     * <p/>
     * If this is provided, the producer will be asynchronous; otherwise, it will be synchronous.
     *
     * @return a factory for asynchronous producers or null, if the producer is to be synchronous.
     *
     * @see KafkaAsyncProducerFactory
     */
    @JsonProperty
    public KafkaAsyncProducerFactory getAsync() {
        return async;
    }

    /**
     * Sets a factory for the asynchronous producer, defaults to synchronous.
     * <p/>
     * If this is provided, the producer will be asynchronous; otherwise, it will be synchronous.
     *
     * @param factory a factory for asynchronous producers or null, if the producer is to be
     *                synchronous.
     *
     * @see KafkaAsyncProducerFactory
     */
    @JsonProperty
    public void setAsync(final KafkaAsyncProducerFactory factory) {
        this.async = factory;
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
