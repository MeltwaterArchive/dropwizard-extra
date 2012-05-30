package com.datasift.dropwizard.conf

import com.yammer.dropwizard.config.Configuration
import javax.validation.Valid
import reflect.BeanProperty
import javax.validation.constraints.NotNull

/** [[com.yammer.dropwizard.config.Configuration]] mix-in for an HBase connection
 *
 * If you wish to configure multiple clients, to connect to multiple clusters
 * for example, you should use [[com.datasift.dropwizard.conf.HBaseConfiguration]]
 * directly and configure each instance manually in your Service.
 */
trait ConfigurableHBaseClient { self: Configuration =>

  /** [[com.yammer.dropwizard.config.Configuration]] for the HBase client */
  @BeanProperty
  @Valid
  val hbase = new HBaseConfiguration
}

/** [[com.yammer.dropwizard.config.Configuration]] for an HBase client */
class HBaseConfiguration extends Configuration {

  // todo: add asynchbase config

  /** configuration for the cluster's ZooKeeper quorum */
  @BeanProperty
  @NotNull
  @Valid
  val zookeeper = new ZooKeeperConfiguration
}
