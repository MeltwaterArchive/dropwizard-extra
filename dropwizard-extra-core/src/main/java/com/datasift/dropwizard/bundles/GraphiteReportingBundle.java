package com.datasift.dropwizard.bundles;

import com.datasift.dropwizard.config.GraphiteConfiguration;
import com.datasift.dropwizard.config.GraphiteReportingConfiguration;
import com.datasift.dropwizard.health.GraphiteHealthCheck;
import com.yammer.dropwizard.ConfiguredBundle;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Configuration;
import com.yammer.dropwizard.config.Environment;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;
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

        final GraphiteConfiguration graphiteConfiguration = configuration.getGraphite();

        if (graphiteConfiguration.getEnabled()) {
            log.info("Reporting metrics to Graphite at {}:{}, every {}",
                    graphiteConfiguration.getHost(),
                    graphiteConfiguration.getPort(),
                    graphiteConfiguration.getFrequency());

            GraphiteReporter.enable(
                    Metrics.defaultRegistry(),
                    graphiteConfiguration.getFrequency().toNanoseconds(),
                    TimeUnit.NANOSECONDS,
                    graphiteConfiguration.getHost(),
                    graphiteConfiguration.getPort(),
                    graphiteConfiguration.getPrefix(),
                    new MetricPredicate() {
                        @Override
                        public boolean matches(final MetricName name, final Metric metric) {
                            return !graphiteConfiguration.getExcludes().contains(pathFor(name));
                        }

                        private String pathFor(final MetricName name) {
                            final StringBuilder sb = new StringBuilder(name.getGroup())
                                    .append('.')
                                    .append(name.getType())
                                    .append('.');
                            if (name.hasScope()) {
                                sb.append(name.getScope()).append('.');
                            }

                            return sb.append(name.getName()).toString();
                        }
                    }
            );

            environment.addHealthCheck(new GraphiteHealthCheck(
                    graphiteConfiguration.getHost(),
                    graphiteConfiguration.getPort(),
                    "graphite"));
        }
    }

    public void initialize(final Bootstrap<?> bootstrap) {
        // nothing doing
    }
}
