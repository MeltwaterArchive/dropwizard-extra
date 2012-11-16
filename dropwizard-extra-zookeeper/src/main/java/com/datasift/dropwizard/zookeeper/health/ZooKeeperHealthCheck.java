package com.datasift.dropwizard.zookeeper.health;

import com.datasift.dropwizard.zookeeper.util.ZNode;
import com.yammer.metrics.core.HealthCheck;
import org.apache.zookeeper.ZooKeeper;

/**
 * A {@link HealthCheck} for a ZooKeeper ensemble.
 * <p/>
 * Checks that the given namespace exists, implicitly checking that the ensemble
 * is reachable.
 */
public class ZooKeeperHealthCheck extends HealthCheck {

    private final ZooKeeper client;
    private final ZNode namespace;

    public ZooKeeperHealthCheck(final ZooKeeper client,
                                final ZNode namespace,
                                final String name) {
        super(name  + " (zookeeper, " + namespace + ")");
        this.client = client;
        this.namespace = namespace;
    }

    @Override
    protected Result check() throws Exception {
        if (client.exists(namespace.toString(), false) == null) {
            return Result.unhealthy("Root namespace does not exist");
        }

        return Result.healthy();
    }
}
