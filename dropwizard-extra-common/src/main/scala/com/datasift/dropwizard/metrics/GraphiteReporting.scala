package com.datasift.dropwizard.metrics

import com.datasift.dropwizard.config.GraphiteReportingConfiguration
import com.yammer.dropwizard.config.Configuration
import com.yammer.dropwizard.{ConfiguredBundle, Bundle, ScalaService}

/** enable reporting to Graphite for a [[com.yammer.dropwizard.AbstractService]] */
trait GraphiteReporting {
  self: TemporaryScalaService[_ <: GraphiteReportingConfiguration] =>

  self.withBundle(new GraphiteReportingBundle)
}

// todo: remove once upgraded to dropwizard 0.4.1
abstract class TemporaryScalaService[T <: Configuration](name: String)
  extends ScalaService[T](name) {

  def withBundle(bundle: Bundle) {
    this.addBundle(bundle)
  }

  def withBundle(bundle: ConfiguredBundle[_ >: T]) {
    this.addBundle(bundle)
  }
}
