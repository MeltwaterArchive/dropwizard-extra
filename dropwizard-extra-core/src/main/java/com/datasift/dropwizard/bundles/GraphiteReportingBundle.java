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
 * <p/>
 * Includes a HealthCheck to the Graphite instance.
 * <p/>
 * To use this {@link ConfiguredBundle}, your {@link Configuration} must implement
 * {@link GraphiteReportingConfiguration}.
 * <p/>
 * <h3>Example</h3>
 * <b>Configuration:</b>
 * <code>
 *     class MyServiceConfiguration
 *         extends Configuration
 *         implements GraphiteReportingConfiguration {
 *
 *         // service specific config
 *         // ...
 *
 *         @JsonProperty
 *         @NotNull
 *         private GraphiteConfiguration graphite;
 *
 *         @Override
 *         public GraphiteConfiguration getGraphite() {
 *             return graphite;
 *         }
 *     }
 * </code>
 * <p/>
 * <b>Service:</b>
 * <code>
 *     class MyService extends Service<MyServiceConfiguration> {
 *
 *         public void initialize(Bootstrap<MyServiceConfiguration> bootstrap) {
 *             bootstrap.addBundle(new GraphiteReportingBundle());
 *         }
 *
 *         public void run(MyServiceConfiguration configuration,
 *                         Environment environment) throws Exception {
 *             // ...
 *         }
 *     }
 * </code>
 *
 */
public class GraphiteReportingBundle implements ConfiguredBundle<GraphiteReportingConfiguration> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * Initializes the Graphite reporter, if enabled.
     *
     * @param configuration the {@link GraphiteReportingConfiguration} to configure the
     *                      {@link GraphiteReporter} with
     * @param environment the {@link Service} environment
     */
    public void run(final GraphiteReportingConfiguration configuration,
                    final Environment environment) {

        if (configuration.getGraphite().getEnabled()) {
            log.info("Reporting metrics to Graphite at {}:{}, every {}",
                    configuration.getGraphite().getHost(),
                    configuration.getGraphite().getPort(),
                    configuration.getGraphite().getFrequency());

            GraphiteReporter.enable(
                    configuration.getGraphite().getFrequency().toNanoseconds(),
                    TimeUnit.NANOSECONDS,
                    configuration.getGraphite().getHost(),
                    configuration.getGraphite().getPort(),
                    configuration.getGraphite().getPrefix()
            );

            environment.addHealthCheck(new GraphiteHealthCheck(
                    configuration.getGraphite().getHost(),
                    configuration.getGraphite().getPort(),
                    "graphite"));
        }
    }

    public void initialize(final Bootstrap<?> bootstrap) {
        // nothing doing
    }
}
