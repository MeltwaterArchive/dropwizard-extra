package com.datasift.dropwizard.bundles;

import com.datasift.dropwizard.config.GraphiteReportingConfiguration;
import com.datasift.dropwizard.health.GraphiteHealthCheck;
import com.yammer.dropwizard.ConfiguredBundle;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Configuration;
import com.yammer.dropwizard.config.Environment;
import com.yammer.metrics.reporting.GraphiteReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * A {@link ConfiguredBundle} for reporting metrics to a remote Graphite server.
 *
 * Includes a HealthCheck to the Graphite instance.
 *
 * To use this {@link ConfiguredBundle}, your {@link Configuration} must
 * implement {@link GraphiteReportingConfiguration}.
 */
public class GraphiteReportingBundle
        implements ConfiguredBundle<GraphiteReportingConfiguration> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * Initializes the Graphite reporter, if enabled.
     *
     * @param conf the {@link GraphiteReportingConfiguration} to configure the
     *             {@link GraphiteReporter} with
     * @param env  the {@link Service} environment
     */
    public void run(GraphiteReportingConfiguration conf, Environment env) throws Exception {

        if (conf.getGraphite().getEnabled()) {
            log.info("Reporting metrics to Graphite at {}:{}, every {}",
                    conf.getGraphite().getHost(),
                    conf.getGraphite().getPort(),
                    conf.getGraphite().getFrequency());

            GraphiteReporter.enable(
                    conf.getGraphite().getFrequency().toNanoseconds(),
                    TimeUnit.NANOSECONDS,
                    conf.getGraphite().getHost(),
                    conf.getGraphite().getPort(),
                    conf.getGraphite().getPrefix()
            );

            env.addHealthCheck(new GraphiteHealthCheck(
                    conf.getGraphite().getHost(),
                    conf.getGraphite().getPort(),
                    "graphite"));
        }
    }

    public void initialize(Bootstrap<?> bootstrap) {
        // nothing doing
    }
}
