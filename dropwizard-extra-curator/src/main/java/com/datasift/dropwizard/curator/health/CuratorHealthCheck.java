package com.datasift.dropwizard.curator.health;

import org.apache.curator.framework.CuratorFramework;
import com.codahale.metrics.health.HealthCheck;

/**
 * A {@link HealthCheck} that ensures a {@link CuratorFramework} is started and that the configured
 * root namespace exists.
 */
public class CuratorHealthCheck extends HealthCheck {

    private final CuratorFramework framework;

    /**
     * Create a new {@link HealthCheck} instance with the given name.
     *
     * @param framework The {@link CuratorFramework} instance to check the health of.
     */
    public CuratorHealthCheck(final CuratorFramework framework) {
        this.framework = framework;
    }

    /**
     * Checks that the {@link CuratorFramework} instance is started and that the configured root
     * namespace exists.
     *
     * @return {@link Result#unhealthy(String)} if the {@link CuratorFramework} is not started or
     *         the configured root namespace does not exist; otherwise, {@link Result#healthy()}.
     * @throws Exception if an error occurs checking the health of the ZooKeeper ensemble.
     */
    @Override
    protected Result check() throws Exception {
        String namespace = framework.getNamespace();
        if (!framework.isStarted()) {
            return Result.unhealthy("Client not started");
        } else if (framework.checkExists().forPath("".equals(namespace) ? "/" : "") == null) {
            return Result.unhealthy("Root for namespace does not exist");
        }

        return Result.healthy();
    }
}
