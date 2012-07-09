package com.datasift.dropwizard.kafka.consumer

import com.yammer.dropwizard.config.Environment
import com.datasift.dropwizard.kafka.config.KafkaConsumerConfiguration
import com.datasift.dropwizard.kafka.KafkaConsumerFactory

/**
 * Factory object for a [[com.datasift.dropwizard.kafka.consumer.KafkaConsumer]]
 */
object Consumer {

  /**
   * Creates a new [[com.datasift.dropwizard.kafka.consumer.KafkaConsumer]] for the given `configuration`.
   *
   * @param conf the configuration to configure the [[com.datasift.dropwizard.kafka.consumer.KafkaConsumer]] with
   * @param env the [[com.yammer.dropwizard.config.Environment]] to manage the [[com.datasift.dropwizard.kafka.consumer.KafkaConsumer]]
   * @return a configured and managed [[com.datasift.dropwizard.kafka.consumer.KafkaConsumer]]
   */
  def apply(conf: KafkaConsumerConfiguration, env: Environment): KafkaConsumer = {
    new KafkaConsumerFactory(env).build(conf)
  }
}
