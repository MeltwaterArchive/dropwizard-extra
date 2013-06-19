package com.datasift.dropwizard.hbase;

import com.codahale.dropwizard.util.Duration;
import com.codahale.dropwizard.util.Size;
import com.codahale.metrics.MetricRegistry;
import com.datasift.dropwizard.hbase.config.HBaseClientConfiguration;
import com.datasift.dropwizard.zookeeper.ZooKeeperFactory;
import com.codahale.dropwizard.setup.Environment;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * A factory for creating and managing {@link HBaseClient} instances.
 * <p/>
 * The implementation of the {@link HBaseClient} is determined by the {@link
 * HBaseClientConfiguration}.
 * <p/>
 * The resulting {@link HBaseClient} will have its lifecycle managed by the {@link Environment} and
 * will have {@link com.codahale.metrics.health.HealthCheck}s installed for the {@code .META.} and
 * {@code -ROOT-} tables.
 *
 * @see HBaseClient
 */
public class HBaseClientFactory {

    private static final String DEFAULT_NAME = "hbase-default";

    /**
     * The ZooKeeper quorum co-ordinating the HBase cluster.
     */
    @JsonProperty
    @NotNull
    @Valid
    protected ZooKeeperFactory zookeeper = new ZooKeeperFactory();

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
     * Whether the {@link HBaseClient} should be instrumented with metrics.
     */
    @JsonProperty
    protected boolean instrumented = true;

    /**
     * @see HBaseClientConfiguration#zookeeper
     */
    public ZooKeeperFactory getZookeeper() {
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

    /**
     * Builds a default {@link HBaseClient} instance from the specified {@link
     * HBaseClientConfiguration}.
     *
     * @param environment the {@link Environment} to build {@link HBaseClient} instances for.
     * @return an {@link HBaseClient}, managed and configured according to the {@code configuration}
     */
    public HBaseClient build(final Environment environment) {
        return build(environment, DEFAULT_NAME);
    }

    /**
     * Builds an {@link HBaseClient} instance from the specified {@link HBaseClientConfiguration}
     * with the given {@code name}.
     *
     * @param environment the {@link Environment} to build {@link HBaseClient} instances for.
     * @param name the name for the {@link HBaseClient}.
     *
     * @return an {@link HBaseClient}, managed and configured according to the {@code
     *         configuration}.
     */
    public HBaseClient build(final Environment environment, final String name) {
        final ZooKeeperFactory zkFactory = getZookeeper();

        final HBaseClient proxy = new HBaseClientProxy(
                new org.hbase.async.HBaseClient(zkFactory.getQuorumSpec(), zkFactory.getNamespace()));

        // optionally instrument and bound requests for the client
        final HBaseClient client = instrument(boundRequests(proxy), environment.metrics(), name);

        // configure client
        client.setFlushInterval(getFlushInterval());
        client.setIncrementBufferSize(getIncrementBufferSize());

        // add healthchecks for META and ROOT tables
        environment.admin().addHealthCheck(name + "-meta", new HBaseHealthCheck(client, ".META."));
        environment.admin().addHealthCheck(name + "-root", new HBaseHealthCheck(client, "-ROOT-"));

        // manage client
        environment.lifecycle().manage(new ManagedHBaseClient(
                client, getConnectionTimeout()));

        return client;
    }

    /**
     * Builds a new {@link HBaseClient} according to the given {@link HBaseClientConfiguration}.
     * <p/>
     * If instrumentation {@link HBaseClientConfiguration#instrumented is enabled} in the
     * configuration, this will build an {@link InstrumentedHBaseClient} wrapping the given {@link
     * HBaseClient}.
     * <p/>
     * If instrumentation is not enabled, the given {@link HBaseClient} will be returned verbatim.
     *
     * @param client an underlying {@link HBaseClient} implementation.
     * @param registry the {@link MetricRegistry} to register metrics with.
     * @param name the name of the client that is being instrumented.
     * @return an {@link HBaseClient} that satisfies the configuration of instrumentation.
     */
    private HBaseClient instrument(final HBaseClient client,
                                   final MetricRegistry registry,
                                   final String name) {
        return isInstrumented()
                ? new InstrumentedHBaseClient(client, registry, name)
                : client;
    }

    /**
     * Builds a new {@link HBaseClient} according to the given {@link HBaseClientConfiguration}.
     * <p/>
     * If the {@link HBaseClientConfiguration#maxConcurrentRequests} is non-zero in the
     * configuration, this will build a {@link BoundedHBaseClient} that wraps the given client.
     * <p/>
     * If {@link HBaseClientConfiguration#maxConcurrentRequests} is zero, the given {@link
     * HBaseClient} will be returned verbatim.
     *
     * @param client an underlying {@link HBaseClient} implementation.
     *
     * @return an {@link HBaseClient} that satisfies the configuration of the maximum concurrent
     *         requests.
     */
    private HBaseClient boundRequests(final HBaseClient client) {
        return getMaxConcurrentRequests() > 0
                ? new BoundedHBaseClient(client, getMaxConcurrentRequests())
                : client;
    }
}
