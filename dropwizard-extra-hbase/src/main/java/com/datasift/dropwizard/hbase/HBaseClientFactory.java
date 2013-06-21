package com.datasift.dropwizard.hbase;

import com.codahale.dropwizard.util.Duration;
import com.codahale.dropwizard.util.Size;
import com.codahale.metrics.MetricRegistry;
import com.datasift.dropwizard.zookeeper.ZooKeeperFactory;
import com.codahale.dropwizard.setup.Environment;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * A factory for creating and managing {@link HBaseClient} instances.
 * <p/>
 * The resulting {@link HBaseClient} will have its lifecycle managed by an {@link Environment} and
 * will have {@link com.codahale.metrics.health.HealthCheck}s installed for the {@code .META.} and
 * {@code -ROOT-} tables.
 *
 * @see HBaseClient
 */
public class HBaseClientFactory {

    private static final String DEFAULT_NAME = "hbase-default";

    @NotNull
    @Valid
    protected ZooKeeperFactory zookeeper = new ZooKeeperFactory();

    @NotNull
    protected Duration flushInterval = Duration.seconds(1);

    @NotNull
    protected Size incrementBufferSize = Size.kilobytes(64);

    @Min(0)
    protected int maxConcurrentRequests = 0;

    @NotNull
    protected Duration connectionTimeout = Duration.seconds(5);

    protected boolean instrumented = true;

    /**
     * Returns the ZooKeeper quorum co-ordinating the HBase cluster.
     *
     * @return the factory for connecting to the ZooKeeper quorum co-ordinating the HBase cluster.
     */
    @JsonProperty
    public ZooKeeperFactory getZookeeper() {
        return zookeeper;
    }

    /**
     * Sets the ZooKeeper quorum co-ordinating the HBase cluster.
     *
     * @param factory a factory for the ZooKeeper quorum co-ordinating the HBase cluster.
     */
    @JsonProperty
    public void setZookeeper(final ZooKeeperFactory factory) {
        this.zookeeper = factory;
    }

    /**
     * Returns the maximum amount of time requests may be buffered client-side before sending them
     * to the server.
     *
     * @return the maximum amount of time requests may be buffered.
     *
     * @see org.hbase.async.HBaseClient#getFlushInterval()
     */
    @JsonProperty
    public Duration getFlushInterval() {
        return flushInterval;
    }

    /**
     * Sets the maximum amount of time requests may be buffered client-side before sending them
     * to the server.
     *
     * @param flushInterval the maximum amount of time requests may be buffered.
     *
     * @see org.hbase.async.HBaseClient#setFlushInterval(short)
     */
    @JsonProperty
    public void setFlushInterval(final Duration flushInterval) {
        this.flushInterval = flushInterval;
    }

    /**
     * Returns the maximum size of the buffer for increment operations.
     * <p/>
     * Once this buffer is full, a flush is forced irrespective of the {@link #getFlushInterval()
     * flushInterval}.
     *
     * @return the maximum number of increments to buffer.
     *
     * @see org.hbase.async.HBaseClient#getIncrementBufferSize()
     */
    @JsonProperty
    public Size getIncrementBufferSize() {
        return incrementBufferSize;
    }

    /**
     * Sets the maximum size of the buffer for increment operations.
     * <p/>
     * Once this buffer is full, a flush is forced irrespective of the {@link #getFlushInterval()
     * flushInterval}.
     *
     * @param incrementBufferSize the maximum number of increments to buffer.
     *
     * @see org.hbase.async.HBaseClient#setIncrementBufferSize(int)
     */
    @JsonProperty
    public void setIncrementBufferSize(final Size incrementBufferSize) {
        this.incrementBufferSize = incrementBufferSize;
    }

    /**
     * Returns maximum number of concurrent asynchronous requests for the client.
     * <p/>
     * Useful for throttling high-throughput applications when HBase is the bottle-neck to prevent
     * the client running out of memory.
     * <p/>
     * With this is zero ("0"), no limit will be placed on the number of concurrent asynchronous
     * requests.
     *
     * @return the maximum number of requests that may be executing concurrently.
     *
     * @see BoundedHBaseClient
     */
    @JsonProperty
    public int getMaxConcurrentRequests() {
        return maxConcurrentRequests;
    }

    /**
     * Sets the maximum number of concurrent asynchronous requests for the client.
     * <p/>
     * Useful for throttling high-throughput applications when HBase is the bottle-neck to prevent
     * the client running out of memory.
     * <p/>
     * With this is zero ("0"), no limit will be placed on the number of concurrent asynchronous
     * requests.
     *
     * @param maxConcurrentRequests the maximum number of requests that may execute concurrently.
     *
     * @see BoundedHBaseClient
     */
    @JsonProperty
    public void setMaxConcurrentRequests(final int maxConcurrentRequests) {
        this.maxConcurrentRequests = maxConcurrentRequests;
    }

    /**
     * Returns the maximum time to wait for a connection to a region server before failing.
     *
     * @return the maximum time to spend connecting to a server before failing.
     */
    @JsonProperty
    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    /**
     * Returns the maximum time to wait for a connection to a region server before failing.
     *
     * @param connectionTimeout the maximum time to spend connecting to a server before failing.
     */
    @JsonProperty
    public void setConnectionTimeout(final Duration connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    /**
     * Returns whether the {@link HBaseClient} should be instrumented with metrics.
     *
     * @return whether the {@link HBaseClient} should be instrumented with metrics.
     */
    @JsonProperty
    public boolean isInstrumented() {
        return instrumented;
    }

    /**
     * Sets whether the {@link HBaseClient} should be instrumented with metrics.
     *
     * @param isInstrumented whether the {@link HBaseClient} should be instrumented with metrics.
     */
    @JsonProperty
    public void setInstrumented(final boolean isInstrumented) {
        this.instrumented = isInstrumented;
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
        environment.healthChecks().register(name + "-meta", new HBaseHealthCheck(client, ".META."));
        environment.healthChecks().register(name + "-root", new HBaseHealthCheck(client, "-ROOT-"));

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
