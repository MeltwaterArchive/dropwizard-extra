package com.datasift.dropwizard.hbase.config

import javax.validation.constraints.{Min, NotNull}
import com.yammer.dropwizard.config.Configuration
import reflect.BeanProperty
import javax.validation.Valid
import com.datasift.dropwizard.zookeeper.config.ZooKeeperConfiguration
import com.yammer.dropwizard.util.Duration

/**[[com.yammer.dropwizard.config.Configuration]] for an HBase client */
class HBaseClientConfiguration extends Configuration {

  /**configuration for the cluster's ZooKeeper quorum */
  @BeanProperty
  @NotNull
  @Valid
  val zookeeper = new ZooKeeperConfiguration

  /** max time an edit may remain buffered client-side before being flushed */
  @BeanProperty
  @NotNull
  @Min(0)
  val flushInterval = Duration.seconds(1)

  /**max number of counter increments to buffer before coalescing and flushing */
  @BeanProperty
  @NotNull
  @Min(0)
  val incrementBufferSize = 65535

  /**max number of concurrent requests awaiting completion before new requests block */
  @BeanProperty
  @NotNull
  @Min(0)
  val maxConcurrentRequests = 0
}
