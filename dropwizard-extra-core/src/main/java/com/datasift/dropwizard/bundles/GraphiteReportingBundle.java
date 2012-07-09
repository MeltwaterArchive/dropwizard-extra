package com.datasift.dropwizard.bundles;

import com.datasift.dropwizard.config.GraphiteReportingConfiguration;
import com.datasift.dropwizard.health.GraphiteHealthCheck;
import com.yammer.dropwizard.ConfiguredBundle;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.logging.Log;
import com.yammer.metrics.reporting.GraphiteReporter;

import java.util.concurrent.TimeUnit;

/**
 * A {@link ConfiguredBundle} for reporting metrics to a remote Graphite server.
 *
 * Includes a HealthCheck to the Graphite instance.
 *
 * To use this {@link ConfiguredBundle}, your {@link com.yammer.dropwizard.config.Configuration}
 * must implement {@link GraphiteReportingConfiguration}.
 */
public class GraphiteReportingBundle implements ConfiguredBundle<GraphiteReportingConfiguration> {

    private Log log = Log.forClass(this.getClass());

    /**
     * Initializes the Graphite reporter, if enabled.
     *
     * @param conf a {@link com.yammer.dropwizard.config.Configuration} that implements {@link GraphiteReportingConfiguration}
     * @param env the {@link com.yammer.dropwizard.Service} environment
     */
    public void initialize(GraphiteReportingConfiguration conf, Environment env) {
        if (conf.getGraphite().getEnabled()) {
            log.info("Graphite metrics reporting enabled to {}:{}, every {} seconds",
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

            env.addHealthCheck(new GraphiteHealthCheck(conf.getGraphite().getHost(), conf.getGraphite().getPort()));
        }
    }
}
