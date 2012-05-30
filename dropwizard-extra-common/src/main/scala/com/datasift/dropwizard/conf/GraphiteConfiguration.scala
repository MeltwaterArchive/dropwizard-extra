package com.datasift.dropwizard.conf

import com.yammer.dropwizard.config.Configuration
import reflect.BeanProperty
import javax.validation.constraints.NotNull
import org.hibernate.validator.constraints.{NotEmpty, Range}
import javax.validation.Valid

/** [[com.yammer.dropwizard.config.Configuration]] mix-in for Graphite support */
trait ConfigurableGraphiteReporting { self: Configuration =>

  /** configuration for Graphite metrics reporting */
  @BeanProperty
  @NotNull
  @Valid
  val graphite: GraphiteConfiguration = new GraphiteConfiguration
}

/** [[com.yammer.dropwizard.config.Configuration]] for a Graphite reporter */
class GraphiteConfiguration extends Configuration {

  /** whether publishing to Graphite is enabled or not */
  @BeanProperty
  val enabled = false

  /** prefix for metric names published to Graphite */
  @BeanProperty
  val prefix = ""

  /** host of Graphite server to publish metrics to */
  @BeanProperty
  @NotEmpty
  val host = ""

  /** port to connect to Graphite server on */
  @BeanProperty
  @NotNull
  @Range(min = 0, max = 49151)
  val port = 8080

  /** frequency to publish metrics to Graphite, in seconds */
  @BeanProperty
  @NotNull
  @Range(min = 1, max = 86400)
  val frequency = 60
}
