package com.datasift.dropwizard.zookeeper;

import com.datasift.dropwizard.zookeeper.config.ZooKeeperConfiguration;
import com.datasift.dropwizard.zookeeper.health.ZooKeeperHealthCheck;
import com.yammer.dropwizard.config.Environment;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * A Factory for creating configured and managed {@link ZooKeeper} client instances.
 * <p/>
 * A {@link ZooKeeperHealthCheck} will be registered for each {@link ZooKeeper} client isntance that
 * checks for the existence of the configured {@link ZooKeeperConfiguration#namespace namespace}.
 *
 * @see ZooKeeperConfiguration
 * @see ZooKeeperHealthCheck
 * @see ManagedZooKeeper
 */
public class ZooKeeperFactory {

    private static final String DEFAULT_NAME = "default";

    private final Environment environment;

    /**
     * Creates a new {@link ZooKeeperFactory} instance for the specified {@link Environment}.
     *
     * @param environment the environment to build {@link ZooKeeper} instances for.
     */
    public ZooKeeperFactory(final Environment environment) {
        this.environment = environment;
    }

    /**
     * Builds a default {@link ZooKeeper} instance from the given {@link ZooKeeperConfiguration}.
     * <p/>
     * No {@link Watcher} will be configured for the built {@link ZooKeeper} instance. If you wish
     * to watch all events on the {@link ZooKeeper} client, use
     * {@link #build(ZooKeeperConfiguration, Watcher)}.
     *
     * @param configuration the configuration to configure the {@link ZooKeeper} instance with.
     *
     * @return a {@link ZooKeeper} client, managed and configured according to the {@code
     *         configuration}.
     *
     * @throws IOException if there is a network failure.
     */
    public ZooKeeper build(final ZooKeeperConfiguration configuration) throws IOException {
        return build(configuration, null, DEFAULT_NAME);
    }

    /**
     * Builds a default {@link ZooKeeper} instance from the given {@link ZooKeeperConfiguration}.
     * <p/>
     * The given {@link Watcher} will be assigned to watch for all events on the {@link ZooKeeper}
     * client instance. If you wish to ignore events, use {@link #build(ZooKeeperConfiguration)}.
     *
     * @param configuration the configuration to configure the {@link ZooKeeper} instance with.
     * @param watcher the watcher to handle all events that occur on the {@link ZooKeeper} client.
     *
     * @return a {@link ZooKeeper} client, managed and configured according to the {@code
     *         configuration}.
     *
     * @throws IOException if there is a network failure.
     */
    public ZooKeeper build(final ZooKeeperConfiguration configuration, final Watcher watcher)
            throws IOException {
        return build(configuration, watcher, DEFAULT_NAME);
    }

    /**
     * Builds a named {@link ZooKeeper} instance from the given {@link ZooKeeperConfiguration}.
     * <p/>
     * No {@link Watcher} will be configured for the built {@link ZooKeeper} instance. If you wish
     * to watch all events on the {@link ZooKeeper} client, use {@link
     * #build(ZooKeeperConfiguration, Watcher, String)}.
     *
     * @param configuration the configuration to configure the {@link ZooKeeper} instance with.
     * @param name the name for this {@link ZooKeeper instance}.
     *
     * @return a {@link ZooKeeper} client, managed and configured according to the {@code
     *         configuration}.
     *
     * @throws IOException if there is a network failure.
     */
    public ZooKeeper build(final ZooKeeperConfiguration configuration, final String name)
            throws IOException {
        return build(configuration, null, name);
    }

    /**
     * Builds a named {@link ZooKeeper} instance from the given {@link ZooKeeperConfiguration}.
     * <p/>
     * The given {@link Watcher} will be assigned to watch for all events on the {@link ZooKeeper}
     * client instance. If you wish to ignore events, use {@link
     * #build(ZooKeeperConfiguration, String)}.
     *
     * @param configuration the configuration to configure the {@link ZooKeeper} instance with.
     * @param watcher the watcher to handle all events that occur on the {@link ZooKeeper} client.
     * @param name the name for this {@link ZooKeeper instance}.
     *
     * @return a {@link ZooKeeper} client, managed and configured according to the {@code
     *         configuration}.
     *
     * @throws IOException if there is a network failure.
     */
    public ZooKeeper build(final ZooKeeperConfiguration configuration,
                           final Watcher watcher,
                           final String name) throws IOException {

        final String quorumSpec = configuration.getQuorumSpec();
        final String namespace = configuration.getNamespace();

        final ZooKeeper client = new ZooKeeper(
                quorumSpec + namespace,
                (int) configuration.getSessionTimeout().toMilliseconds(),
                watcher,
                configuration.canBeReadOnly());

        final ZooKeeperConfiguration.Auth auth = configuration.getAuth();
        if (auth != null) {
            client.addAuthInfo(auth.getScheme(), auth.getId());
        }

        environment.getAdminEnvironment().addHealthCheck(new ZooKeeperHealthCheck(client, quorumSpec, namespace, name));
        environment.getLifecycleEnvironment().manage(new ManagedZooKeeper(client));

        return client;
    }
}
