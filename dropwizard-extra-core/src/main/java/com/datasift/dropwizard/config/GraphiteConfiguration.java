package com.datasift.dropwizard.config;

import com.datasift.dropwizard.bundles.GraphiteReportingBundle;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.codahale.dropwizard.util.Duration;
import org.hibernate.validator.constraints.NotEmpty;
import org.hibernate.validator.constraints.Range;

import javax.validation.constraints.NotNull;

/**
 * Configuration for a Graphite metrics reporter.
 *
 * @see com.codahale.metrics.graphite.GraphiteReporter
 * @see GraphiteReportingBundle
 */
public class GraphiteConfiguration {

    /**
     * Whether reporting metrics to Graphite should be enabled.
     */
    @JsonProperty
    protected boolean enabled = false;

    /**
     * The prefix (if any) for key names sent to Graphite.
     */
    @JsonProperty
    @NotNull
    protected String prefix = "";

    /**
     * The hostname of the Graphite server to report metrics to.
     */
    @JsonProperty
    @NotEmpty
    protected String host = "localhost";

    /**
     * The port of the Graphite server to report metrics to.
     */
    @JsonProperty
    @NotNull
    @Range(min = 0, max = 49151)
    protected int port = 8080;

    /**
     * The frequency to report metrics to Graphite.
     */
    @JsonProperty
    @NotNull
    protected Duration frequency = Duration.minutes(1);

    /**
     * The set of metrics to exclude from being reported to Graphite.
     * <p/>
     * The metric is specified as its fully-qualified path in Graphite, excluding the {@link
     * #prefix}: <code>package.class[.scope].name</code>
     */
    @JsonProperty
    @NotNull
    protected ImmutableSet<String> excludes = ImmutableSet.of();

    /**
     * @see GraphiteConfiguration#enabled
     */
    public boolean getEnabled() {
        return enabled;
    }

    /**
     * @see GraphiteConfiguration#prefix
     */
    public String getPrefix() {
        return prefix;
    }

    /**
     * @see GraphiteConfiguration#host
     */
    public String getHost() {
        return host;
    }

    /**
     * @see GraphiteConfiguration#port
     */
    public int getPort() {
        return port;
    }

    /**
     * @see GraphiteConfiguration#frequency
     */
    public Duration getFrequency() {
        return frequency;
    }

    /**
     * @see GraphiteConfiguration#excludes
     */
    public ImmutableSet<String> getExcludes() {
        return excludes;
    }
}
