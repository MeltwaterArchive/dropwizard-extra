package com.datasift.dropwizard.logging.gelf;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.yammer.dropwizard.logging.LoggingOutput;
import com.yammer.dropwizard.util.Size;
import org.hibernate.validator.constraints.NotEmpty;
import org.hibernate.validator.constraints.Range;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.Map;
import java.util.Set;

/**
 * A {@link LoggingOutput} for writing log messages to a graylog2 server.
 *
 * TODO: write our own Appender to avoid the dependency on Gson.
 */
@JsonTypeName("gelf")
public class GelfLoggingOutput implements LoggingOutput {

    /**
     * The hostname of the Graylog2 server to send log messages to.
     */
    @JsonProperty
    @NotEmpty
    private String host;

    /**
     * The port of the Graylog2 server to send log messages to.
     */
    @JsonProperty
    @Range(min = 1024, max = 65535)
    private int port;

    /**
     * The maximum size of each GELF chunk to send to the server.
     */
    @JsonProperty
    @NotNull
    private Size chunkSize = Size.kilobytes(1);

    /**
     * The format of the log message as a Logback log pattern.
     */
    @JsonProperty
    private String logFormat;

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

    /**
     * Builds an {@link Appender} for writing log messages to a graylog2 server.
     *
     * @param context the context of the Logger to build the Appender for.
     * @param serviceName the name of the service to build the Appender for.
     *
     * @return an Appender for writing log messages to a graylog2 server.
     */
    @Override
    public Appender<ILoggingEvent> build(final LoggerContext context,
                                         final String serviceName) {

        final GelfLayout layout = new GelfLayout(new ObjectMapper()); // todo: find a way to provide this from the Environment?

        // use service name for GELF facility
        layout.setFacility(serviceName);
        layout.setHostname(getLocalHost());
        layout.setContext(context);
        layout.setAdditionalFields(includes);

        // configure log format only when defined
        if (!Strings.isNullOrEmpty(logFormat)) {
            layout.setPattern(logFormat);
        }

        final GelfAppender<ILoggingEvent> appender = new GelfAppender<ILoggingEvent>();

        appender.setLayout(layout);
        appender.setContext(context);
        appender.setHostname(host);
        appender.setPort(port);
        appender.setChunkSize((int) chunkSize.toBytes());

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
