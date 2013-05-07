package com.datasift.dropwizard.zookeeper.health;

import com.codahale.metrics.health.HealthCheck;
import org.apache.zookeeper.ZooKeeper;

/**
 * A {@link HealthCheck} for a ZooKeeper ensemble.
 * <p/>
 * Checks that:
 * <ul>
 *     <li>the client is alive,</li>
 *     <li>the client is connected to the cluster, and</li>
 *     <li>the configured namespace exists</li>
 * </ul>
 */
public class ZooKeeperHealthCheck extends HealthCheck {

    private final ZooKeeper client;
    private final String namespace;

    /**
     * Creates a {@link HealthCheck} that checks the given {@link ZooKeeper} client is functioning
     * correctly.
     *
     * @param client the client to check the health of.
     * @param namespace the namespace to check for within the ZooKeeper ensemble.
     * @param name the name of this {@link HealthCheck}.
     */
    public ZooKeeperHealthCheck(final ZooKeeper client, final String namespace) {
        this.client = client;
        this.namespace = namespace;
    }

    /**
     * Checks that the configured {@link ZooKeeper} client is functioning correctly.
     *
     * @return {@link Result#unhealthy(String)} if the client is not functioning correctly or the
     *         connected ZooKeeper ensemble is not operating correctly; otherwise, {@link
     *         Result#healthy()}.
     *
     * @throws Exception if an error occurs checking the health of the {@link ZooKeeper} client.
     */
    @Override
    protected Result check() throws Exception {

        final ZooKeeper.States state = client.getState();

        if (!state.isAlive()) {
            return Result.unhealthy("Client is dead, in state: %s", state);
        }

        if (!state.isConnected()) {
            return Result.unhealthy("Client not connected, in state: %s", state);
        }

        if (client.exists(namespace, false) == null) {
            return Result.unhealthy("Root namespace does not exist: %s", namespace);
        }

        return Result.healthy();
    }
}
