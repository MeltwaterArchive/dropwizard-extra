package com.datasift.dropwizard.health;

import com.yammer.metrics.core.HealthCheck;
import com.yammer.metrics.core.HealthCheck.Result;

import java.io.IOException;
import java.net.Socket;

/**
 * A {@link HealthCheck} for remote sockets.
 *
 * Use this as a basis for {@link HealthCheck}s for remote services, such as
 * databases or web-services.
 */
public abstract class SocketHealthCheck extends HealthCheck {

    private final String host;
    private final int port;

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public SocketHealthCheck(final String host,
                             final int port,
                             final String name) {
        super(String.format("%s-%s:%d", name, host, port));
        this.host = host;
        this.port = port;
    }

    public String toString() {
        return getHost() + ":" + getPort();
    }

    @Override
    protected Result check() throws Exception {
        final Socket socket = createSocket(host, port);
        return socket.isConnected()
                ? check(socket)
                : Result.unhealthy(String.format(
                        "Failed to connect to %s:%d", host, port));
    }

    protected Socket createSocket(final String host,
                                  final int port) throws IOException {
        return new Socket(host, port);
    }

    /**
     * Perform a check of a {@link Socket}.
     * <p>
     * The socket is provided to implementations already connected.
     *
     * @param socket the {@link Socket} to check the health of
     * @return if the component is healthy, a healthy {@link Result};
     *         otherwise, an unhealthy {@link Result} with a
     *         descriptive error message or exception
     * @throws Exception if there is an unhandled error during the health check;
     *                   this will result in a failed health check
     */
    protected abstract Result check(Socket socket);
}
