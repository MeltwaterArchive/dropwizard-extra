package com.datasift.dropwizard.logging.gelf;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Layout;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.codahale.dropwizard.logging.AppenderFactory;
import com.codahale.dropwizard.util.Size;
import org.hibernate.validator.constraints.NotEmpty;
import org.hibernate.validator.constraints.Range;

import javax.validation.constraints.NotNull;
import java.net.*;
import java.util.Enumeration;

/**
 * An {@link AppenderFactory} for writing log messages to a remote GELF server.
 *
 * @see https://github.com/Graylog2/graylog2-docs/wiki/GELF
 */
@JsonTypeName("gelf")
public class GelfAppenderFactory implements AppenderFactory {

    @NotEmpty
    private String host = "localhost";

    @Range(min = 1024, max = 65535)
    private int port = 12201;

    @NotNull
    private Size chunkSize = Size.kilobytes(1);

    private Optional<String> logFormat = Optional.absent();

    @NotNull
    private String[] includes = new String[0];

    /**
     * Returns the hostname of the GELF server to send log messages to.
     *
     * @return the hostname of the GELF server to send log messages to.
     */
    @JsonProperty
    public String getHost() {
        return host;
    }

    /**
     * Sets the hostname of the GELF server to send log messages to.
     *
     * @param host the hostname of the GELF server to send log messages to.
     */
    @JsonProperty
    public void setHost(final String host) {
        this.host = host;
    }

    /**
     * Returns the port of the GELF server to send log messages to.
     *
     * @return the port of the GELF server to send log messages to.
     */
    @JsonProperty
    public int getPort() {
        return port;
    }

    /**
     * Sets the port of the GELF server to send log messages to.
     *
     * @param port the port of the GELF server to send log messages to.
     */
    @JsonProperty
    public void setPort(final int port) {
        this.port = port;
    }

    /**
     * Returns the maximum size of each batch of messages sent to the server.
     *
     * @return the maximum size of each batch of messages sent to the server.
     */
    @JsonProperty
    public Size getChunkSize() {
        return chunkSize;
    }

    /**
     * Sets the maximum size of each batch of messages sent to the server.
     *
     * @param size the maximum size of each batch of messages sent to the server.
     */
    @JsonProperty
    public void setChunkSize(final Size size) {
        this.chunkSize = size;
    }

    /**
     * Returns the optional format of the log message, as a Logback log pattern.
     *
     * @return the format of the log message, as a Logback pattern, if present.
     */
    @JsonProperty
    public Optional<String> getLogFormat() {
        return logFormat;
    }

    /**
     * Sets the optional format of the log message, as a Logback log pattern.
     *
     * @param format the format of the log message, as a Logback pattern, or {@link
     *               Optional#absent()}. for the default log format.
     */
    @JsonProperty
    public void setLogFormat(final String format) {
        this.logFormat = Optional.fromNullable(format);
    }

    /**
     * Returns the SLF4J MDC fields to include in the messages sent via GELF.
     *
     * <b>Example, adding an IP address for requests</b>
     * In application code:
     * <code>
     *     org.slf4j.MDC.put("ipAddress", getClientIpAddress());
     * </code>
     * In configuration (YAML):
     * <code>
     *     logging:
     *         outputs:
     *             - type: "gelf"
     *             ...
     *             - includes: [ "ipAddress" ]
     * </code>
     *
     * @return the SLF4J named MDC fields that will be included in messages logged via GELF.
     */
    @JsonProperty
    public String[] getIncludes() {
        return includes;
    }

    /**
     * Sets the SLF4J MDC fields to include in the messages sent via GELF.
     *
     * <b>Example, adding an IP address for requests</b>
     * In application code:
     * <code>
     *     org.slf4j.MDC.put("ipAddress", getClientIpAddress());
     * </code>
     * In configuration (YAML):
     * <code>
     *     logging:
     *         outputs:
     *             - type: "gelf"
     *             ...
     *             - includes: [ "ipAddress" ]
     * </code>
     *
     * @param includes the SLF4J named MDC fields that will be included in messages logged via GELF.
     */
    @JsonProperty
    public void setIncludes(final String[] includes) {
        this.includes = includes;
    }

    /**
     * Builds an {@link Appender} for writing log messages to a graylog2 server.
     *
     * @param context the context of the Logger to build the Appender for.
     * @param serviceName the name of the service to build the Appender for.
     * @param layout an optional {@link Layout} for overriding the layout of log messages.
     *
     * @return an Appender for writing log messages to a graylog2 server.
     */
    @Override
    public Appender<ILoggingEvent> build(final LoggerContext context,
                                         final String serviceName,
                                         final Layout<ILoggingEvent> layout) {
        final GelfAppender<ILoggingEvent> appender = new GelfAppender<>();

        // todo: find a way to provide ObjectMapper from the Environment?
        final GelfLayout formatter = new GelfLayout(new ObjectMapper());

        // use service name for GELF facility
        formatter.setFacility(serviceName);
        formatter.setHostname(getLocalHost());
        formatter.setContext(context);
        formatter.setAdditionalFields(getIncludes());

        // configure log format only when defined
        if (getLogFormat().isPresent()) {
            formatter.setPattern(getLogFormat().get());
        }

        // use the custom layout, if provided
        appender.setLayout(layout);

        appender.setLayout(formatter);
        appender.setContext(context);
        appender.setHostname(getHost());
        appender.setPort(getPort());
        appender.setChunkSize((int) getChunkSize().toBytes());

        appender.start();

        return appender;
    }

    // Attempts to determine a valid local address for this application.
    // This is done by opening a connection to the target machine to determine the hostname of the
    // network interface that made the connection. If no connection can be made, 0.0.0.0 will be
    // returned as the address.
    private String getLocalHost() {
        try {
            final DatagramSocket socket = new DatagramSocket();
            socket.connect(new InetSocketAddress(getHost(), getPort()));
            return socket.getLocalAddress() != null
                    ? socket.getLocalAddress().getCanonicalHostName()
                    : "0.0.0.0";
        } catch (final SocketException e) {
            return "0.0.0.0";
        }
    }
}
