package com.datasift.dropwizard.logging.gelf;

import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.util.LevelToSyslogSeverity;
import ch.qos.logback.core.Layout;
import ch.qos.logback.core.LayoutBase;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Iterators;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.Iterator;
import java.util.Map;

/**
 * A Logback {@link Layout} for the GELF (Graylog2 Extended Logging Format).
 */
public class GelfLayout extends LayoutBase<ILoggingEvent> {

    private static final int MAX_SHORT_LENGTH = 255;

    private final ObjectMapper mapper;
    private Layout<ILoggingEvent> layout = null;

    private String hostname = "localhost";
    private String facility = "GELF";
    private String[] additionalFields = new String[0];

    public GelfLayout() {
        this(new ObjectMapper());
    }

    public GelfLayout(final ObjectMapper mapper) {
        this.mapper = mapper;
        setLayout(null);
    }

    public void setLayout(final Layout<ILoggingEvent> layout) {
        if (layout == null) {
            setPattern("%m%rEx");
        } else {
            this.layout = layout;
        }
    }

    public void setPattern(final String pattern) {
        final PatternLayout layout = new PatternLayout();
        layout.setPattern(pattern);
        setLayout(layout);
    }

    public void setHostname(final String hostname) {
        this.hostname = hostname;
    }

    public void setFacility(final String facility) {
        this.facility = facility;
    }

    public void setAdditionalFields(final String[] additionalFields) {
        this.additionalFields = additionalFields;
    }

    @Override
    public String getContentType() {
        return "application/json+gelf";
    }

    @Override
    public String doLayout(final ILoggingEvent event) {
        final GelfEvent ev = new GelfEvent(event);

        final ObjectNode tree = mapper.valueToTree(ev);

        // add standard extras (logger and thread names)
        tree.put("_loggerName", event.getLoggerName());
        tree.put("_threadName", event.getThreadName());

        // add any MDC properties (filtering if requested)
        final Map<String, String> mdc = event.getMDCPropertyMap();
        final Iterator<String> it = additionalFields.length == 0
                ? mdc.keySet().iterator()
                : Iterators.forArray(additionalFields);
        while (it.hasNext()) {
            final String key = it.next();
            final String value = mdc.get(key);

            // the spec forbids "id" keys as they conflict with MongoDB's book-keeping...
            if (!"id".equals(key) && value != null) {
                tree.put("_" + key, value);
            }
        }

        try {
            return mapper.writeValueAsString(tree);
        } catch (final JsonProcessingException e) {
            // todo: figure out if this is the right course of action
            throw new RuntimeException("Failed to format log event for GELF", e);
        }
    }

    /**
     * A representation of GELF events for JSON serialization.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private class GelfEvent {

        @JsonProperty
        private final String version = "1.0";

        @JsonProperty
        private final String host = hostname;

        @JsonProperty("short_message")
        private final String shortMessage;

        @JsonProperty("full_message")
        private final String fullMessage;

        @JsonProperty
        private final double timestamp;

        @JsonProperty
        private final int level;

        @JsonProperty
        private final String facility = GelfLayout.this.facility;

        @JsonProperty
        private final String file;

        @JsonProperty
        private final Integer line;

        GelfEvent(final ILoggingEvent event) {
            final String message = layout.doLayout(event);

            this.shortMessage = message.length() > MAX_SHORT_LENGTH
                            ? message.substring(0, MAX_SHORT_LENGTH)
                            : message;
            this.fullMessage = message;
            this.timestamp = event.getTimeStamp() / 1000;
            this.level = LevelToSyslogSeverity.convert(event);

            final StackTraceElement[] trace = event.getCallerData();
            this.file = trace.length > 0 ? trace[0].getFileName() : null;
            this.line = trace.length > 0 ? trace[0].getLineNumber() : null;
        }
    }
}
