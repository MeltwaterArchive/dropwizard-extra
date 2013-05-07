package com.datasift.dropwizard.config;

import com.codahale.dropwizard.Configuration;

/**
 * Interface for {@link Configuration}s that include Graphite metrics reporting capability.
 */
public interface GraphiteReportingConfiguration {

    /**
     * Gets the configuration of the Graphite instance to report metrics to.
     *
     * @return the configuration of the Graphite instance to report metrics to.
     */
    public GraphiteConfiguration getGraphite();
}
