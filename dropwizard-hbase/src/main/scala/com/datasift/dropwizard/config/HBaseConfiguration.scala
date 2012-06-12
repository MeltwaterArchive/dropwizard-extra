package com.datasift.dropwizard.config

import com.yammer.dropwizard.config.Configuration
import javax.validation.Valid
import reflect.BeanProperty

/** [[com.yammer.dropwizard.config.Configuration]] mix-in for an HBase connection
 *
 * If you wish to configure multiple clients you should use
 * [[com.datasift.dropwizard.config.HBaseClientConfiguration]] directly and
 * configure each instance manually in your Service.
 */
trait HBaseConfiguration {
  self: Configuration =>

  /** [[com.datasift.dropwizard.config.HBaseClientConfiguration]] for the HBase client */
  @BeanProperty
  @Valid
  val hbase = new HBaseClientConfiguration
}
