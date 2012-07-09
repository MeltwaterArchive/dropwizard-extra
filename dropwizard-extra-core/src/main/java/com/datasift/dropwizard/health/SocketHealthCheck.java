package com.datasift.dropwizard.health;

import com.yammer.metrics.core.HealthCheck;

import java.net.Socket;

/**
 * A {@link HealthCheck} for a remote system
 *
 * Use this as a basis for healthchecks for remote services, such as databases
 * or web-services.
 */
abstract public class SocketHealthCheck extends HealthCheck {

    private String host;
    private int port;

    /**
     * Create a new {@link HealthCheck} instance for a remote socket.
     *
     * @param host the hostname of the remote socket to connect to
     * @param port the port of the remote socket to connect to
     * @param name the name of the health check (and, ideally, the name of the
     *             underlying component the health check tests)
     */
    protected SocketHealthCheck(String host, int port, String name) {
        super(new StringBuilder(name)
                .append(": ")
                .append(host)
                .append(":")
                .append(port).toString());

        this.host = host;
        this.port = port;
    }

    /**
     * Perform a check of the application component.
     *
     * @return if the component is healthy, a healthy {@link Result};
     *         otherwise, an unhealthy {@link Result} with a
     *         descriptive error message or exception
     *
     * @throws Exception if there is an unhandled error during the health check;
     *                   this will result in a failed health check
     */
    protected Result check() throws Exception {
        Socket socket = new Socket(host, port);
        if (socket.isConnected()) {
            return check(socket);
        } else {
            return Result.unhealthy("Not connected: unknown problem");
        }
    }

    /**
     * Perform a check of a remote component
     *
     * A socket that is already connected to the component is provided as a
     * convenience to implementations. You may ignore it and handle the connection
     * yourself safely.
     *
     * @param socket a {@link Socket} connected to the remote component
     * @return if the component is healthy, a healthy {@link Result};
     *         otherwise, an unhealthy {@link Result} with a
     *         descriptive error message or exception
     *
     * @throws Exception if there is an unhandled error during the health check;
     *                   this will result in a failed health check
     */
    protected abstract Result check(Socket socket);
}
