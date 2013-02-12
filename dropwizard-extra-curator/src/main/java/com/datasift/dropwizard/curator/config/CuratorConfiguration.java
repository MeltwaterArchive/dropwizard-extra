package com.datasift.dropwizard.curator.config;

import com.datasift.dropwizard.zookeeper.config.ZooKeeperConfiguration;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yammer.dropwizard.util.Duration;

import javax.validation.constraints.NotNull;

/**
 * A configuration for a {@link com.netflix.curator.framework.CuratorFramework} instance.
 */
public class CuratorConfiguration {

    /**
     * The configuration of the ZooKeeper ensemble to use.
     */
    @JsonProperty
    @NotNull
    protected ZooKeeperConfiguration ensemble = new ZooKeeperConfiguration();

    /**
     * The policy to take in the event of failures.
     * <p/>
     * The following options are available:
     * <ul>
     *     <li><i>Exponential Back-off</i> -
     *     "backoff [upto] &lt;N&gt; times [sleeping [for] M (seconds|millis|...)"</li>
     *     <li><i>Fixed Retries</i> - "[retry] N times [sleeping [for] M (seconds|millis|...)"</li>
     *     <li><i>Timed Retries</i> -
     *     "[retry] until N (seconds|milli|...) [sleeping [for] M (seconds|millis|...)"</li>
     * </ul>
     *
     * @see RetryPolicy#parse(String)
     */
    @JsonProperty
    @NotNull
    protected RetryPolicy retryPolicy = RetryPolicy.times(5, Duration.seconds(3));

    public ZooKeeperConfiguration getEnsembleConfiguration() {
        return ensemble;
    }

    public com.netflix.curator.RetryPolicy getRetryPolicy() {
        return retryPolicy.getPolicy();
    }
}
