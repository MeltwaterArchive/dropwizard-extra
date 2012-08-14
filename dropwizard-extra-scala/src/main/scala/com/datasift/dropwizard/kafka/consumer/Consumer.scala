package com.datasift.dropwizard.kafka.consumer

import com.yammer.dropwizard.config.Environment
import com.datasift.dropwizard.kafka.config.KafkaConsumerConfiguration
import com.datasift.dropwizard.kafka.KafkaConsumerFactory
import kafka.serializer.Decoder
import kafka.message.Message

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
  def apply[A : Decoder](conf: KafkaConsumerConfiguration, env: Environment)
                        (f: StreamProcessor[A]): KafkaConsumer = {
    new KafkaConsumerFactory(env)
      .processWith(implicitly[Decoder[A]], f)
      .build(conf)
  }

  /**
   * Creates a new [[com.datasift.dropwizard.kafka.consumer.KafkaConsumer]] for the given `configuration`.
   *
   * @param conf the configuration to configure the [[com.datasift.dropwizard.kafka.consumer.KafkaConsumer]] with
   * @param env the [[com.yammer.dropwizard.config.Environment]] to manage the [[com.datasift.dropwizard.kafka.consumer.KafkaConsumer]]
   * @return a configured and managed [[com.datasift.dropwizard.kafka.consumer.KafkaConsumer]]
   */
  def apply[A : Decoder](conf: KafkaConsumerConfiguration,
                         env: Environment,
                         name: String)
                        (f: StreamProcessor[A]): KafkaConsumer = {
    new KafkaConsumerFactory(env)
      .processWith(implicitly[Decoder[A]], f)
      .build(conf, name)
  }
}
