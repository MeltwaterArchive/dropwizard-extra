package com.datasift.dropwizard.config;

import com.yammer.dropwizard.config.Configuration;

/**
 * Interface for {@link Configuration}s that include Graphite metrics reporting
 * capability.
 */
public interface GraphiteReportingConfiguration {

    public GraphiteConfiguration getGraphite();
}
