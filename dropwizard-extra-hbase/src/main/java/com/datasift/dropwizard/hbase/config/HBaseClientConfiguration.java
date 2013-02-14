package com.datasift.dropwizard.hbase.config;

import com.datasift.dropwizard.hbase.HBaseClient;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.datasift.dropwizard.zookeeper.config.ZooKeeperConfiguration;
import com.yammer.dropwizard.util.Duration;
import com.yammer.dropwizard.util.Size;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * Configuration for an {@link HBaseClient}.
 */
public class HBaseClientConfiguration {

    /**
     * The ZooKeeper quorum co-ordinating the HBase cluster.
     *
     * @see ZooKeeperConfiguration
     */
    @JsonProperty
    @NotNull
    @Valid
    protected ZooKeeperConfiguration zookeeper = new ZooKeeperConfiguration();

    /**
     * The maximum amount of time requests may be buffered client-side before sending them to the
     * server.
     *
     * @see org.hbase.async.HBaseClient#setFlushInterval(short)
     */
    @JsonProperty
    @NotNull
    protected Duration flushInterval = Duration.seconds(1);

    /**
     * The maximum size of the buffer for increment operations.
     * <p/>
     * Once this buffer is full, a flush is forced irrespective of the {@link
     * HBaseClientConfiguration#flushInterval flushInterval}.
     *
     * @see org.hbase.async.HBaseClient#setIncrementBufferSize(int)
     */
    @JsonProperty
    @NotNull
    protected Size incrementBufferSize = Size.kilobytes(64);

    /**
     * The maximum number of concurrent asynchronous requests for the client.
     * <p/>
     * Useful for throttling high-throughput applications when HBase is the bottle-neck to prevent
     * the client running out of memory.
     * <p/>
     * With this is zero ("0"), no limit will be placed on the number of concurrent asynchronous
     * requests.
     *
     * @see com.datasift.dropwizard.hbase.BoundedHBaseClient
     */
    @JsonProperty
    @Min(0)
    protected int maxConcurrentRequests = 0;

    /**
     * The maximum time to wait for a connection to a region server before failing.
     */
    @JsonProperty
    @NotNull
    protected Duration connectionTimeout = Duration.seconds(5);

    /**
     * Whether the {@link HBaseClient} should be instrumented with {@link
     * com.yammer.metrics.core.Metric}s.
     */
    @JsonProperty
    protected boolean instrumented = true;

    /**
     * @see HBaseClientConfiguration#zookeeper
     */
    public ZooKeeperConfiguration getZookeeper() {
        return zookeeper;
    }

    /**
     * @see HBaseClientConfiguration#flushInterval
     */
    public Duration getFlushInterval() {
        return flushInterval;
    }

    /**
     * @see HBaseClientConfiguration#incrementBufferSize
     */
    public Size getIncrementBufferSize() {
        return incrementBufferSize;
    }

    /**
     * @see HBaseClientConfiguration#maxConcurrentRequests
     */
    public int getMaxConcurrentRequests() {
        return maxConcurrentRequests;
    }

    /**
     * @see HBaseClientConfiguration#connectionTimeout
     */
    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    /**
     * @see HBaseClientConfiguration#instrumented
     */
    public boolean isInstrumented() {
        return instrumented;
    }
}
