package com.datasift.dropwizard.kafka.config

import com.yammer.dropwizard.config.Configuration
import reflect.BeanProperty
import javax.validation.Valid
import javax.validation.constraints.NotNull
import com.datasift.dropwizard.zookeeper.config.ZooKeeperConfiguration
import com.yammer.dropwizard.util.Duration

/** [[com.yammer.dropwizard.config.Configuration]] for connecting to a Kafka cluster */
class KafkaClientConfiguration extends Configuration {

  /** configuration for the cluster's ZooKeeper quroum */
  @BeanProperty
  @NotNull
  @Valid
  val zookeeper = new ZooKeeperConfiguration

  /** client socket timeout, in milliseconds */
  @BeanProperty
  @NotNull
  val socketTimeout = Duration.seconds(30)

  /** producer-specific configuration */
  @BeanProperty
  @Valid
  @NotNull
  val producer = new KafkaProducerConfiguration

  /** consumer-specific configuration */
  @BeanProperty
  @Valid
  @NotNull
  val consumer = new KafkaConsumerConfiguration
}
