package com.datasift.dropwizard.bundles;

import com.codahale.dropwizard.util.Duration;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.graphite.Graphite;
import com.datasift.dropwizard.config.GraphiteConfiguration;
import com.datasift.dropwizard.config.GraphiteReportingConfiguration;
import com.datasift.dropwizard.health.GraphiteHealthCheck;
import com.codahale.dropwizard.ConfiguredBundle;
import com.codahale.dropwizard.Service;
import com.codahale.dropwizard.setup.Bootstrap;
import com.codahale.dropwizard.Configuration;
import com.codahale.dropwizard.setup.Environment;
import com.codahale.metrics.Metric;
import com.codahale.metrics.graphite.GraphiteReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

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
 *         \@JsonProperty
 *         \@NotNull
 *         private GraphiteConfiguration graphite;
 *
 *         \@Override
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
        final String host = graphiteConfiguration.getHost();
        final int port = graphiteConfiguration.getPort();
        final Duration frequency = graphiteConfiguration.getFrequency();
        final String prefix = graphiteConfiguration.getPrefix();

        if (graphiteConfiguration.getEnabled()) {
            log.info("Reporting metrics to Graphite at {}:{}, every {}", host, port, frequency);

            final GraphiteReporter reporter = GraphiteReporter
                    .forRegistry(environment.metrics())
                    .prefixedWith(prefix)
                    .filter(new MetricFilter() {
                        @Override
                        public boolean matches(final String name, final Metric metric) {
                            return !graphiteConfiguration.getExcludes().contains(name);
                        }
                    })
                    .build(new Graphite(new InetSocketAddress(host, port)));

            reporter.start(frequency.getQuantity(), frequency.getUnit());

            environment.admin().addHealthCheck("graphite", new GraphiteHealthCheck(host, port));
        }
    }

    public void initialize(final Bootstrap<?> bootstrap) {
        // nothing doing
    }
}
