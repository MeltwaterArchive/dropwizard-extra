package com.datasift.dropwizard.zookeeper.health;

import com.netflix.curator.framework.CuratorFramework;
import com.yammer.metrics.core.HealthCheck;

/**
 * A {@link HealthCheck} that ensures a {@link CuratorFramework} is started and
 * that the configured root namespace exists.
 */
public class CuratorHealthCheck extends HealthCheck {

    private final CuratorFramework framework;

    /**
     * Create a new {@link HealthCheck} instance with the given name.
     *
     * @param name the name of the health check (ideally, the name of the
     *             underlying ZooKeeper ensemble this HealthCheck tests)
     */
    public CuratorHealthCheck(final CuratorFramework framework,
                                 final String name) {
        super(name + " (zookeeper)");
        this.framework = framework;
    }

    @Override
    protected Result check() throws Exception {
        if (!framework.isStarted()) {
            return Result.unhealthy("Client not started");
        } else if (framework.checkExists().forPath("/") == null) {
            return Result.unhealthy("Root namespace does not exist");
        }

        return Result.healthy();
    }
}
