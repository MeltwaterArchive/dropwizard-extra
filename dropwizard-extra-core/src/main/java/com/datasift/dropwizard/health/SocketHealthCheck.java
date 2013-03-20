package com.datasift.dropwizard.health;

import com.yammer.metrics.core.HealthCheck;

import java.io.IOException;
import java.net.Socket;

/**
 * A base {@link HealthCheck} for remote socket servers.
 * <p/>
 * Use this as a basis for {@link HealthCheck}s for remote services, such as databases or
 * web-services.
 */
public abstract class SocketHealthCheck extends HealthCheck {

    private final String hostname;
    private final int port;

    /**
     * Gets the hostname of the remote socket being checked.
     *
     * @return the hostname of the remote socket being checked.
     */
    public String getHostname() {
        return hostname;
    }

    /**
     * Gets the port of the remote socket being checked.
     *
     * @return the port of the remote socket being checked.
     */
    public int getPort() {
        return port;
    }

    /**
     * Initialises a {@link HealthCheck} for a remote socket with the given {@code hostname} and
     * {@code port}.
     *
     * @param hostname the hostname of the remote socket to check.
     * @param port the port of the remote socket to check.
     * @param name the name of this {@link HealthCheck}.
     */
    public SocketHealthCheck(final String hostname, final int port, final String name) {
        super(String.format("%s-%s:%d", name, hostname, port));
        this.hostname = hostname;
        this.port = port;
    }

    /**
     * Generates a String representation of the remote socket being checked.
     * <p/>
     * This will be the socket address formatted as: hostname:port
     *
     * @return the String representation of the remote socket being checked.
     */
    public String toString() {
        return getHostname() + ":" + getPort();
    }

    /**
     * Checks that the configured remote socket can be connected to.
     *
     * @return The result of {@link #check(Socket)} if the socket can be connected to; or
     *         {@link Result#unhealthy(String)} if the socket connection fails.
     *
     * @throws Exception Any Exceptions thrown by the implementation of {@link #check(Socket)}.
     */
    @Override
    protected Result check() throws Exception {
        final Socket socket = createSocket(hostname, port);
        return socket.isConnected()
                ? check(socket)
                : Result.unhealthy(String.format(
                        "Failed to connect to %s:%d", hostname, port));
    }

    /**
     * Creates a new {@link Socket} for the given {@code hostname} and {@code port}.
     *
     * @param hostname the remote hostname of the {@link Socket} to create.
     * @param port the remote port of the {@link Socket} to create.
     *
     * @return a new {@link Socket} for the given {@code hostname} and {@code port}.
     *
     * @throws IOException if an I/O error occurs when creating the {@link Socket} or connecting it.
     */
    protected Socket createSocket(final String hostname, final int port) throws IOException {
        return new Socket(hostname, port);
    }

    /**
     * Perform a check of a {@link Socket}.
     * <p/>
     * Implementations can assume that the {@link Socket} is already connected.
     *
     * @param socket the {@link Socket} to check the health of
     *
     * @return if the component is healthy, a healthy {@link Result}; otherwise, an unhealthy {@link
     *         Result} with a description of the error or exception
     *
     * @throws Exception if there is an unhandled error during the health check; this will result in
     *                   a failed health check
     */
    protected abstract Result check(Socket socket);
}
