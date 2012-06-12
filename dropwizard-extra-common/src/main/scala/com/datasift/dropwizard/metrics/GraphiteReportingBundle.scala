package com.datasift.dropwizard.metrics

import com.yammer.metrics.reporting.GraphiteReporter
import java.util.concurrent.TimeUnit
import com.datasift.dropwizard.health.GraphiteHealthCheck
import com.datasift.dropwizard.config.GraphiteReportingConfiguration
import com.yammer.dropwizard.config.Environment
import com.yammer.dropwizard.{ConfiguredBundle, Logging}

/** enable reporting to Graphite for a [[com.yammer.dropwizard.AbstractService]] */
class GraphiteReportingBundle
  extends ConfiguredBundle[GraphiteReportingConfiguration] with Logging {

  def initialize(conf: GraphiteReportingConfiguration, env: Environment) {
    if (conf.graphite.enabled) {
      log.info("Graphite metrics reporting enabled to {}:{}, every {} seconds",
        conf.graphite.host,
        conf.graphite.port.toString,
        conf.graphite.frequency.toString)

      GraphiteReporter.enable(
        conf.graphite.frequency,
        TimeUnit.SECONDS,
        conf.graphite.host,
        conf.graphite.port,
        conf.graphite.prefix
      )

      env.addHealthCheck(new GraphiteHealthCheck(conf.graphite.host, conf.graphite.port))
    }
  }
}


