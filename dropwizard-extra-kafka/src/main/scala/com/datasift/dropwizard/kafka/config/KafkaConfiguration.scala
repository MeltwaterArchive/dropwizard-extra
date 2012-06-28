package com.datasift.dropwizard.kafka.config

import com.yammer.dropwizard.config.Configuration
import reflect.BeanProperty
import javax.validation.Valid
import javax.validation.constraints.NotNull

/** Configuration mix-in for a Kafka cluster */
trait KafkaConfiguration {
  self: Configuration =>

  @BeanProperty
  @Valid
  @NotNull
  val kafka = new KafkaClientConfiguration
}
