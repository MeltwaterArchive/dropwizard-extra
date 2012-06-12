package com.datasift.dropwizard.config

import com.yammer.dropwizard.config.Configuration
import reflect.BeanProperty
import javax.validation.Valid
import javax.validation.constraints.NotNull

/** [[com.yammer.dropwizard.config.Configuration]] mix-in for Graphite support */
trait GraphiteReportingConfiguration {
  self: Configuration =>

  /** configuration for Graphite metrics reporting */
  @BeanProperty
  @NotNull
  @Valid
  val graphite: GraphiteConfiguration = new GraphiteConfiguration
}
