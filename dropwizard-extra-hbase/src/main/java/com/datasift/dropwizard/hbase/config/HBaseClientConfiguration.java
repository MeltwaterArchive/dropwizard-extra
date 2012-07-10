package com.datasift.dropwizard.hbase.config;

import com.datasift.dropwizard.config.ZooKeeperConfiguration;
import com.yammer.dropwizard.util.Duration;
import com.yammer.dropwizard.util.Size;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * Configuration for an {@link com.datasift.dropwizard.hbase.HBaseClient}
 */
public class HBaseClientConfiguration {

    @JsonProperty
    @NotNull
    @Valid
    protected ZooKeeperConfiguration zookeeper = new ZooKeeperConfiguration();

    @JsonProperty
    @NotNull
    protected Duration flushInterval = Duration.seconds(1);

    @JsonProperty
    @NotNull
    protected Size incrementBufferSize = Size.kilobytes(64);

    @JsonProperty
    @Min(0)
    protected int maxConcurrentRequests = 0;

    @JsonProperty
    @NotNull
    protected Duration connectionTimeout = Duration.seconds(5);

    @JsonProperty
    protected boolean instrumented = true;

    public ZooKeeperConfiguration getZookeeper() {
        return zookeeper;
    }

    public Duration getFlushInterval() {
        return flushInterval;
    }

    public Size getIncrementBufferSize() {
        return incrementBufferSize;
    }

    public int getMaxConcurrentRequests() {
        return maxConcurrentRequests;
    }

    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    public boolean isInstrumented() {
        return instrumented;
    }
}
