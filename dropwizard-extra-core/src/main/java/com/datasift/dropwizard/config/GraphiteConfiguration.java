package com.datasift.dropwizard.config;

import com.yammer.dropwizard.util.Duration;
import org.codehaus.jackson.annotate.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;
import org.hibernate.validator.constraints.Range;

import javax.validation.constraints.NotNull;

/**
 * Configuration for a Graphite metrics reporter.
 *
 * @see com.yammer.metrics.reporting.GraphiteReporter
 */
public class GraphiteConfiguration {

    @JsonProperty
    protected boolean enabled = false;

    @JsonProperty
    @NotNull
    protected String prefix = "";

    @JsonProperty
    @NotEmpty
    protected String host = "localhost";

    @JsonProperty
    @NotNull
    @Range(min = 0, max = 49151)
    protected int port = 8080;

    @JsonProperty
    @NotNull
    protected Duration frequency = Duration.minutes(1);

    public boolean getEnabled() {
        return enabled;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public Duration getFrequency() {
        return frequency;
    }
}
