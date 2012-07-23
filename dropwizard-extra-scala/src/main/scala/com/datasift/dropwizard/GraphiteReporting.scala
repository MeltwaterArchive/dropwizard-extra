package com.datasift.dropwizard

import bundles.GraphiteReportingBundle
import com.yammer.dropwizard.ScalaService
import config.GraphiteReportingConfiguration

/** A `Service` mix-in for the `GraphiteReportingBundle` */
trait GraphiteReporting {
  this: ScalaService[_ <: GraphiteReportingConfiguration] =>

  withBundle(new GraphiteReportingBundle())
}
