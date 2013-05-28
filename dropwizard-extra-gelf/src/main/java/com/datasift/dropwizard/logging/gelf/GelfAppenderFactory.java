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
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

/**
 * An {@link AppenderFactory} for writing log messages to a graylog2 server.
 */
@JsonTypeName("gelf")
public class GelfAppenderFactory implements AppenderFactory {

    /**
     * The hostname of the Graylog2 server to send log messages to.
     */
    @NotEmpty
    private String host = "localhost";

    /**
     * The port of the Graylog2 server to send log messages to.
     */
    @Range(min = 1024, max = 65535)
    private int port = 12201;

    /**
     * The maximum size of each GELF chunk to send to the server.
     */
    @NotNull
    private Size chunkSize = Size.kilobytes(1);

    /**
     * The format of the log message as a Logback log pattern.
     */
    private Optional<String> logFormat = Optional.absent();

    /**
     * SLF4J MDC fields to include in the messages sent via GELF.
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
     */
    @JsonProperty
    private String[] includes = new String[0];

    @JsonProperty
    public String getHost() {
        return host;
    }

    @JsonProperty
    public void setHost(final String host) {
        this.host = host;
    }

    @JsonProperty
    public int getPort() {
        return port;
    }

    @JsonProperty
    public void setPort(final int port) {
        this.port = port;
    }

    @JsonProperty
    public Size getChunkSize() {
        return chunkSize;
    }

    @JsonProperty
    public void setChunkSize(final Size chunkSize) {
        this.chunkSize = chunkSize;
    }

    @JsonProperty
    public Optional<String> getLogFormat() {
        return logFormat;
    }

    @JsonProperty
    public void setLogFormat(final String logFormat) {
        this.logFormat = Optional.fromNullable(logFormat);
    }

    @JsonProperty
    public String[] getIncludes() {
        return includes;
    }

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

    private String getLocalHost() {
        try {
            final Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
            NetworkInterface iface = null;
            while (iface == null && ifaces != null && ifaces.hasMoreElements()) {
                iface = ifaces.nextElement();
            }

            if (iface == null) {
                // cannot find a network interface
                return "0.0.0.0";
            }

            final Enumeration<InetAddress> addresses = iface.getInetAddresses();
            InetAddress address = null;
            while (address == null && addresses.hasMoreElements()) {
                address = addresses.nextElement();
            }

            return address == null
                    ? "0.0.0.0" // no network interface has a valid network address
                    : address.getHostAddress();
        } catch (final SocketException se) {
            return "0.0.0.0"; // cannot inspect network interfaces
        }
    }
}
