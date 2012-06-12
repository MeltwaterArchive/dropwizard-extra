package com.datasift.dropwizard.kafka.consumer

import java.util.Properties
import kafka.consumer.{ConsumerConfig, ConsumerConnector}
import com.datasift.dropwizard.config.KafkaConfiguration

/**Factory object for initializing a Kafka Consumer */
object Consumer {

  /**Creates a Consumer defined by a ConsumerConfig */
  def apply(conf: ConsumerConfig): ConsumerConnector = {
    kafka.consumer.Consumer.create(conf)
  }

  /**Creates a Consumer defined by some Properties */
  def apply(props: Properties): ConsumerConnector = {
    apply(new ConsumerConfig(props))
  }

  /**Creates a Consumer defined by a KafkaConsumerConfiguration */
  def apply(conf: KafkaConfiguration): ConsumerConnector = {
    apply(conf.toProperties)
  }
}
