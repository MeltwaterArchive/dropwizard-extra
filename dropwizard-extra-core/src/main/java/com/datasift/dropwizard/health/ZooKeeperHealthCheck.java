package com.datasift.dropwizard.health;

import com.yammer.metrics.core.HealthCheck;

import java.net.Socket;

/**
 * A {@link HealthCheck} for a quorum of ZooKeeper nodes.
 */
public class ZooKeeperHealthCheck extends SocketHealthCheck {

    public ZooKeeperHealthCheck(final String host,
                                final int port,
                                final String name) {
        super(host, port, name);
    }

    /**
     * Checks that a ZooKeeper quorum node is operating correctly.
     *
     * @param socket a {@link Socket} connected to the ZooKeeper quorum node
     * @return Healthy if the ZooKeeper quorum node is operating correctly;
     *         otherwise, unhealthy
     */
    @Override
    protected Result check(final Socket socket) {
        // todo: find a way to verify the node is responding correctly
        return Result.healthy();
    }
}
