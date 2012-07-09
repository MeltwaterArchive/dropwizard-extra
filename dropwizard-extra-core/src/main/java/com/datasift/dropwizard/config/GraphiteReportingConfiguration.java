package com.datasift.dropwizard.config;

/**
 * Interface for {@link com.yammer.dropwizard.config.Configuration}s that include Graphite reporting.
 */
public interface GraphiteReportingConfiguration {

    public GraphiteConfiguration getGraphite();
}
