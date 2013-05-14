package com.datasift.dropwizard.hbase;

import com.codahale.metrics.MetricRegistry;
import com.datasift.dropwizard.hbase.config.HBaseClientConfiguration;
import com.datasift.dropwizard.zookeeper.config.ZooKeeperConfiguration;
import com.codahale.dropwizard.setup.Environment;

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

    private final Environment environment;
    private static final String DEFAULT_NAME = "hbase-default";

    /**
     * Creates a new {@link HBaseClientFactory} instance for the specified {@link Environment}.
     *
     * @param environment the {@link Environment} to build {@link HBaseClient} instances for
     */
    public HBaseClientFactory(final Environment environment) {
        this.environment = environment;
    }

    /**
     * Builds a default {@link HBaseClient} instance from the specified {@link
     * HBaseClientConfiguration}.
     *
     * @param configuration the {@link HBaseClientConfiguration} to configure an {@link HBaseClient}
     * @return an {@link HBaseClient}, managed and configured according to the {@code configuration}
     */
    public HBaseClient build(final HBaseClientConfiguration configuration) {
        return build(configuration, DEFAULT_NAME);
    }

    /**
     * Builds an {@link HBaseClient} instance from the specified {@link HBaseClientConfiguration}
     * with the given {@code name}.
     *
     * @param configuration the {@link HBaseClientConfiguration} for the {@link HBaseClient}.
     * @param name the name for the {@link HBaseClient}.
     *
     * @return an {@link HBaseClient}, managed and configured according to the {@code
     *         configuration}.
     */
    public HBaseClient build(final HBaseClientConfiguration configuration, final String name) {
        final ZooKeeperConfiguration zkConfiguration = configuration.getZookeeper();

        final HBaseClient proxy = new HBaseClientProxy(
                new org.hbase.async.HBaseClient(
                        zkConfiguration.getQuorumSpec(),
                        zkConfiguration.getNamespace()));

        // optionally instrument and bound requests for the client
        final HBaseClient client = instrument(
                configuration,
                boundRequests(configuration, proxy),
                environment.metrics(),
                name);

        // configure client
        client.setFlushInterval(configuration.getFlushInterval());
        client.setIncrementBufferSize(configuration.getIncrementBufferSize());

        // add healthchecks for META and ROOT tables
        environment.admin().addHealthCheck(name + "-meta", new HBaseHealthCheck(client, ".META."));
        environment.admin().addHealthCheck(name + "-root", new HBaseHealthCheck(client, "-ROOT-"));

        // manage client
        environment.lifecycle().manage(new ManagedHBaseClient(
                client, configuration.getConnectionTimeout()));

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
     * @param configuration an {@link HBaseClientConfiguration} defining the {@link HBaseClient}s
     *                      parameters.
     * @param client an underlying {@link HBaseClient} implementation.
     * @param registry the {@link MetricRegistry} to register metrics with.
     * @param name the name of the client that is being instrumented.
     * @return an {@link HBaseClient} that satisfies the configuration of instrumentation.
     */
    private HBaseClient instrument(final HBaseClientConfiguration configuration,
                                   final HBaseClient client,
                                   final MetricRegistry registry,
                                   final String name) {
        return configuration.isInstrumented()
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
     * @param configuration an {@link HBaseClientConfiguration} defining the {@link HBaseClient}s
     *                      parameters.
     * @param client an underlying {@link HBaseClient} implementation.
     *
     * @return an {@link HBaseClient} that satisfies the configuration of the maximum concurrent
     *         requests.
     */
    private HBaseClient boundRequests(final HBaseClientConfiguration configuration,
                                      final HBaseClient client) {
        return configuration.getMaxConcurrentRequests() > 0
                ? new BoundedHBaseClient(client, configuration.getMaxConcurrentRequests())
                : client;
    }
}
