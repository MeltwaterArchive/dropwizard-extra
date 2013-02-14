package com.datasift.dropwizard.logging;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.yammer.dropwizard.logging.LoggingOutput;
import com.yammer.dropwizard.util.Size;
import me.moocar.logbackgelf.GelfAppender;
import org.hibernate.validator.constraints.NotEmpty;
import org.hibernate.validator.constraints.Range;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import java.util.Map;

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
     * The version of the Graylog2 server.
     *
     * Defaults to 0.9.5.
     */
    @JsonProperty
    @Pattern(regexp = "[0-9]+\\.[0-9]+(\\.[0-9]+)?")
    private String graylogVersion = "0.9.5";

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
     * Additional fields for the log message as a mapping of the SLF4J MDC name
     * to the GELF field.
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
     *             - additionalFields:
     *                 ipAddress: _ip_address
     * </code>
     * By convention, GELF fields <i>should</i> begin with an underscore.
     */
    @JsonProperty
    private Map<String, String> additionalFields = ImmutableMap.of();

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

        final GelfAppender<ILoggingEvent> appender = new GelfAppender<ILoggingEvent>();

        // use service name for GELF facility
        appender.setFacility(serviceName);

        // configure GELF Appender
        appender.setGraylog2ServerHost(host);
        appender.setGraylog2ServerPort(port);
        appender.setChunkThreshold((int) chunkSize.toBytes());
        appender.setUseLoggerName(true);
        appender.setUseThreadName(true);
        appender.setAdditionalFields(additionalFields);

        // configure log format only when defined
        if (!Strings.isNullOrEmpty(logFormat)) {
            appender.setMessagePattern(logFormat);
        }

        // configure protocol version only when defined
        if (!Strings.isNullOrEmpty(graylogVersion)) {
            appender.setGraylog2ServerVersion(graylogVersion);
        }

        // configure name and context
        appender.setName("GELF");
        appender.setContext(context);

        appender.start();

        return appender;
    }
}
