package com.datasift.dropwizard.kafka.config;

import com.google.common.collect.ImmutableMap;
import com.yammer.dropwizard.util.Duration;
import com.yammer.dropwizard.util.Size;
import org.codehaus.jackson.annotate.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.Map;

/**
 * TODO: Document
 */
public class KafkaConsumerConfiguration extends KafkaClientConfiguration {

    /**consumer group this process belongs to */
    @JsonProperty
    @NotEmpty
    protected String group = "";

    /** mapping of the number of partitions to consume for each topic */
    @JsonProperty
    @NotNull
    protected Map<String, Integer> partitions = ImmutableMap.of();

    /**consumer timeout, in milliseconds */
    @JsonProperty
    protected Duration timeout = null;

    /**consumer socket buffer size, in bytes */
    @JsonProperty
    @NotNull
    protected Size receiveBufferSize = Size.kilobytes(64);

    /**maximum size of each response to a fetch request from a broker */
    @JsonProperty
    @NotNull
    protected Size fetchSize = Size.kilobytes(300);

    /**milliseconds to back-off polling broker when receiving no data */
    @JsonProperty
    @NotNull
    protected Duration backOffIncrement = Duration.seconds(1);

    /**maximum number of chunks to queue in internal buffers */
    @JsonProperty
    @Min(0)
    protected int queuedChunks = 100;

    /**automatically commit offsets periodically, disable for manual commit */
    @JsonProperty
    protected boolean autoCommit = true;

    /**frequency to automatically commit offsets if autocommit is enabled */
    @JsonProperty
    @NotNull
    protected Duration autoCommitInterval = Duration.seconds(10);

    /**max number of retries during a rebalance */
    @JsonProperty
    @Min(0)
    protected int rebalanceRetries = 4;

    @JsonProperty
    @NotNull
    protected Duration shutdownPeriod = Duration.seconds(5);

    /** action to take in the event of an error */
    @JsonProperty
    @NotNull
    protected ErrorPolicy errorPolicy = ErrorPolicy.DEFAULT;

    public String getGroup() {
        return group;
    }

    public Map<String, Integer> getPartitions() {
        return partitions;
    }

    public Duration getTimeout() {
        return timeout == null ? Duration.milliseconds(-1) : timeout;
    }

    public Size getReceiveBufferSize() {
        return receiveBufferSize;
    }

    public Size getFetchSize() {
        return fetchSize;
    }

    public Duration getBackOffIncrement() {
        return backOffIncrement;
    }

    public int getQueuedChunks() {
        return queuedChunks;
    }

    public boolean getAutoCommit() {
        return autoCommit;
    }

    public Duration getAutoCommitInterval() {
        return autoCommitInterval;
    }

    public int getRebalanceRetries() {
        return rebalanceRetries;
    }

    public ErrorPolicy getErrorPolicy() {
        return errorPolicy;
    }

    public Duration getShutdownPeriod() {
        return shutdownPeriod;
    }
}
