package com.datasift.dropwizard.kafka.config

import com.yammer.dropwizard.config.Configuration
import reflect.BeanProperty
import javax.validation.Valid
import com.datasift.dropwizard.zookeeper.config.ZooKeeperConfiguration
import com.yammer.dropwizard.util.Duration

/**General client [[com.yammer.dropwizard.config.Configuration]] for a Kafka cluster */
class KafkaConfiguration extends Configuration {

  /**configuration for the cluster's ZooKeeper quroum */
  @BeanProperty
  @NotNull
  @Valid
  val zookeeper = new ZooKeeperConfiguration

  /**consumer socket timeout, in milliseconds */
  @BeanProperty
  @NotNull
  val socketTimeout = Duration.seconds(30)

}
