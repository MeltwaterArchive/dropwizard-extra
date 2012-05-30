package com.datasift.dropwizard.metrics

import com.yammer.metrics.reporting.GraphiteReporter
import java.util.concurrent.TimeUnit
import com.datasift.dropwizard.conf.ConfigurableGraphiteReporting
import com.datasift.dropwizard.ComposableService
import com.datasift.dropwizard.health.GraphiteHealthCheck
import com.yammer.dropwizard.config.Environment
import com.yammer.dropwizard.Logging

/** enable reporting to Graphite for a [[com.datasift.dropwizard.ComposableService]] */
trait GraphiteReporting {
  self: ComposableService[_ <: ConfigurableGraphiteReporting] with Logging =>

  self afterInit { (conf: ConfigurableGraphiteReporting, env: Environment) =>
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
