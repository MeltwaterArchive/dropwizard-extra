package com.datasift.dropwizard.kafka.config;

import com.datasift.dropwizard.kafka.compression.Compression;
import com.yammer.dropwizard.util.Duration;
import com.yammer.dropwizard.util.Size;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * TODO: Document
 */
public class KafkaProducerConfiguration extends KafkaClientConfiguration {

    /** socket send buffer size */
    @JsonProperty
    @NotNull
    protected Size sendBufferSize = Size.kilobytes(10);

    /** maximum time to wait on connection to a broker before failing with an error */
    @JsonProperty
    @NotNull
    protected Duration connectionTimeout = Duration.milliseconds(5000);

    /** number of produce requests after which the conection to the broker is refreshed */
    @JsonProperty
    @Min(0)
    protected long reconnectInterval = 30000;

    /** maximum size of a message payload */
    @JsonProperty
    @NotNull
    protected Size maxMessageSize = Size.megabytes(1);

    /** compression codec for compressing all messages */
    @JsonProperty
    protected Compression compression = Compression.parse("none");

    /** optional list of the topics to compress */
    @JsonProperty
    protected String[] compressedTopics = new String[0];

    /** number of retries for refreshing the partition cache after a cache miss */
    @JsonProperty
    @Min(0)
    protected int partitionMissRetries = 3;

    /** Configuration for the asynchronous producer, defaults to synchronous */
    @JsonProperty
    protected KafkaAsyncProducerConfiguration async = null;

    public Size getSendBufferSize() {
        return sendBufferSize;
    }

    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    public long getReconnectInterval() {
        return reconnectInterval;
    }

    public Size getMaxMessageSize() {
        return maxMessageSize;
    }

    public Compression getCompression() {
        return compression == null ? Compression.parse("default") : compression;
    }

    public String[] getCompressedTopics() {
        return compressedTopics == null ? new String[0] : compressedTopics;
    }

    public int getPartitionMissRetries() {
        return partitionMissRetries;
    }

    public KafkaAsyncProducerConfiguration getAsync() {
        return async;
    }

    public boolean isAsync() {
        return async != null;
    }
}
