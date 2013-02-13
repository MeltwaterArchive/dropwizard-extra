package com.datasift.dropwizard.logging;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.yammer.dropwizard.logging.LoggingOutput;
import me.moocar.logbackgelf.GelfAppender;
import org.hibernate.validator.constraints.NotEmpty;
import org.hibernate.validator.constraints.Range;

import javax.validation.constraints.Min;
import javax.validation.constraints.Pattern;
import java.util.Map;

/**
 * A {@link LoggingOutput} for writing log messages to a graylog2 server.
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

    @JsonProperty
    @Min(1000)
    private int chunkThreshold = 1000;

    @JsonProperty
    private String logFormat;

    @JsonProperty
    private Map<String, String> additionalFields = ImmutableMap.of();

    @Override
    public Appender<ILoggingEvent> build(final LoggerContext context,
                                         final String serviceName) {

        final GelfAppender<ILoggingEvent> appender = new GelfAppender<ILoggingEvent>();
        appender.setFacility(serviceName);
        appender.setGraylog2ServerHost(host);
        appender.setGraylog2ServerPort(port);
        appender.setUseLoggerName(true);
        appender.setUseThreadName(true);
        appender.setAdditionalFields(additionalFields);

        if (!Strings.isNullOrEmpty(logFormat)) {
            appender.setMessagePattern(logFormat);
        }

        if (!Strings.isNullOrEmpty(graylogVersion)) {
            appender.setGraylog2ServerVersion(graylogVersion);
        }

        return appender;
    }
}
