package com.datasift.dropwizard.hbase;

import com.datasift.dropwizard.hbase.config.HBaseClientConfiguration;
import com.datasift.dropwizard.zookeeper.config.ZooKeeperConfiguration;
import com.datasift.dropwizard.zookeeper.health.ZooKeeperHealthCheck;
import com.yammer.dropwizard.config.Environment;
import org.apache.zookeeper.ZooKeeper;

/**
 * A factory for creating and managing {@link HBaseClient} instances.
 * <p>
 * The implementation of the {@link HBaseClient} is determined by the
 * {@link HBaseClientConfiguration}.
 * <p>
 * The resulting {@link HBaseClient} will have its lifecycle managed by the
 * {@link Environment} and will have {@link com.yammer.metrics.core.HealthCheck}s
 * installed for the {@code .META.} and {@code -ROOT-} tables.
 *
 * @see HBaseClient
 */
public class HBaseClientFactory {

    private final Environment environment;

    /**
     * Creates a new {@link HBaseClientFactory} instance for the specified
     * {@link Environment}.
     *
     * @param environment the {@link Environment} to build {@link HBaseClient}
     *                    instances for
     */
    public HBaseClientFactory(final Environment environment) {
        this.environment = environment;
    }

    /**
     * Builds a default {@link HBaseClient} instance from the specified
     * {@link HBaseClientConfiguration}.
     *
     * @param configuration the {@link HBaseClientConfiguration} to configure an
     *                      {@link HBaseClient}
     * @return an {@link HBaseClient}, managed and configured according to the
     *            {@code configuration}
     */
    public HBaseClient build(final HBaseClientConfiguration configuration) {
        return build(configuration, "default");
    }

    /**
     * Builds an {@link HBaseClient} instance from the specified
     * {@link HBaseClientConfiguration} with the given {@code name}.
     *
     * @param configuration the {@link HBaseClientConfiguration} to configure an
     *                      {@link HBaseClient}
     * @param name the name for the {@link HBaseClient}
     * @return an {@link HBaseClient}, managed and configured according to the
     *         {@code configuration}
     */
    public HBaseClient build(final HBaseClientConfiguration configuration,
                             final String name) {
        final ZooKeeperConfiguration zkConfiguration = configuration.getZookeeper();

        final HBaseClient proxy = new HBaseClientProxy(
                new org.hbase.async.HBaseClient(
                        zkConfiguration.getQuorumSpec(),
                        zkConfiguration.getNamespace().toString()));

        // wrap the client conditionally, based on the configuration
        final HBaseClient client =
                InstrumentedHBaseClient.wrap(configuration,
                    BoundedHBaseClient.wrap(configuration,
                            proxy));

        // configure client
        client.setFlushInterval(configuration.getFlushInterval());
        client.setIncrementBufferSize(configuration.getIncrementBufferSize());

        // add healthchecks for META and ROOT tables
        environment.addHealthCheck(new HBaseHealthCheck(client, name, ".META."));
        environment.addHealthCheck(new HBaseHealthCheck(client, name, "-ROOT-"));

        // manage client
        environment.manage(new ManagedHBaseClient(
                client, configuration.getConnectionTimeout()));

        return client;
    }
}
