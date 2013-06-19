package com.datasift.dropwizard.kafka.consumer

import com.codahale.dropwizard.setup.Environment
import com.datasift.dropwizard.kafka.KafkaConsumerFactory
import kafka.serializer.Decoder

/**
 * Factory object for a [[com.datasift.dropwizard.kafka.consumer.KafkaConsumer]]
 */
object Consumer {

  /**
   * Creates a new [[com.datasift.dropwizard.kafka.consumer.KafkaConsumer]] for the given `configuration`.
   *
   * @param env the [[com.codahale.dropwizard.setup.Environment]] to manage the [[com.datasift.dropwizard.kafka.consumer.KafkaConsumer]]
   * @return a configured and managed [[com.datasift.dropwizard.kafka.consumer.KafkaConsumer]]
   */
  def apply[A : Decoder](env: Environment)(f: StreamProcessor[A]): KafkaConsumer = {
    new KafkaConsumerFactory().processWith(implicitly[Decoder[A]], f).build(env)
  }

  /**
   * Creates a new [[com.datasift.dropwizard.kafka.consumer.KafkaConsumer]] for the given `configuration`.
   *
   * @param env the [[com.codahale.dropwizard.setup.Environment]] to manage the [[com.datasift.dropwizard.kafka.consumer.KafkaConsumer]]
   * @return a configured and managed [[com.datasift.dropwizard.kafka.consumer.KafkaConsumer]]
   */
  def apply[A : Decoder](env: Environment, name: String)(f: StreamProcessor[A]): KafkaConsumer = {
    new KafkaConsumerFactory().processWith(implicitly[Decoder[A]], f).build(env, name)
  }
}
