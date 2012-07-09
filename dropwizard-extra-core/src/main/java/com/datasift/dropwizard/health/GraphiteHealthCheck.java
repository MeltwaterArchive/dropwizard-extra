package com.datasift.dropwizard.health;

import com.yammer.metrics.core.HealthCheck;

import java.net.Socket;

/**
 * A {@link HealthCheck} for a remote Graphite instance.
 */
public class GraphiteHealthCheck extends SocketHealthCheck {

    /**
     * Create a new {@link HealthCheck} instance for a remote Graphite server.
     *
     * @param host the hostname of the remote socket to connect to
     * @param port the port of the remote socket to connect to
     */
    public GraphiteHealthCheck(String host, int port) {
        super(host, port, "Graphite");
    }

    @Override
    protected Result check(Socket socket) {
        // TODO: find a way to verify the socket is connected to a Graphite server
        return Result.healthy();
    }
}
