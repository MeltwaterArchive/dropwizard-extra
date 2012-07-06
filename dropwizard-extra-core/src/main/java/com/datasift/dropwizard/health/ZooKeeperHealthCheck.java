package com.datasift.dropwizard.health;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.yammer.metrics.core.HealthCheck;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

/**
 * A {@link HealthCheck} for a quorum of ZooKeeper nodes.
 */
public class ZooKeeperHealthCheck extends HealthCheck {

    private String[] hosts;
    private int port;

    /**
     * Create a new {@link com.yammer.metrics.core.HealthCheck} instance for a ZooKeeper cluster.
     *
     * @param hosts the hosts in the ZooKeeper quorum
     * @param port  the port to connect to the ZooKeeper hosts on
     */
    public ZooKeeperHealthCheck(String[] hosts, int port) {
        super(formatQuorumSpec(hosts, port, "ZooKeeper: "));
        this.hosts = hosts;
        this.port = port;
    }

    @Override
    protected Result check() throws Exception {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (String host : hosts) {
            try {
                // TODO: find a way to ensure we're connected to a ZK node and that it's healthy
                if (!new Socket(host, port).isConnected()) {
                    builder.add(host + ":" + port);
                }
            } catch (IOException e) {
                builder.add(host + ":" + port);
            }
        }
        List<String> deadHosts = builder.build();
        Joiner joiner = Joiner.on(", ");

        if (deadHosts.isEmpty()) {
            return Result.healthy();
        } else if (deadHosts.size() < hosts.length) {
            return Result.unhealthy("Unable to connect to some nodes in quorum: " + joiner.join(deadHosts));
        } else {
            return Result.unhealthy("Unable to connect to any nodes in quorum: " + joiner.join(deadHosts));
        }
    }

    private static String formatQuorumSpec(String[] hosts, int port, String prefix) {
        return Joiner
                .on(":" + port)
                .skipNulls()
                .appendTo(new StringBuilder(prefix == null ? "" : prefix), hosts)
                .append(':')
                .append(port)
                .toString();
    }
}
