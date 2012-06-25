package com.datasift.dropwizard.metrics

import config.GraphiteReportingConfiguration
import com.yammer.dropwizard.ScalaService

/** enable reporting to Graphite for a [[com.yammer.dropwizard.AbstractService]] */
trait GraphiteReporting {
  self: ScalaService[_ <: GraphiteReportingConfiguration] =>

  self.withBundle(new GraphiteReportingBundle)
}
