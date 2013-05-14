package com.datasift.dropwizard.kafka.consumer

import com.codahale.dropwizard.setup.Environment
import com.datasift.dropwizard.kafka.config.KafkaConsumerConfiguration
import com.datasift.dropwizard.kafka.KafkaConsumerFactory
import kafka.serializer.Decoder

/** Factory object for a [[com.datasift.dropwizard.kafka.consumer.KafkaConsumer]]. */
object Consumer {

  /** Creates a [[com.datasift.dropwizard.kafka.consumer.KafkaConsumer]] for the given
    * configuration and [[com.datasift.dropwizard.kafka.consumer.StreamProcessor]].
    *
    * @tparam A type of the messages that are to be consumed.
    * @param conf configuration to configure the consumer with.
    * @param env environment to manage the consumer lifecycle.
    * @param f a [[com.datasift.dropwizard.kafka.consumer.StreamProcessor]] to process the decoded
    *          messages.
    * @return a configured and managed [[com.datasift.dropwizard.kafka.consumer.KafkaConsumer]].
    */
  def apply[A : Decoder](conf: KafkaConsumerConfiguration, env: Environment)
                        (f: StreamProcessor[A]): KafkaConsumer = {
    new KafkaConsumerFactory(env).processWith(implicitly[Decoder[A]], f).build(conf)
  }

  /** Creates a [[com.datasift.dropwizard.kafka.consumer.KafkaConsumer]] for the given
    * configuration and [[com.datasift.dropwizard.kafka.consumer.StreamProcessor]].
    *
    * @tparam A type of the messages that are to be consumed.
    * @param conf configuration to configure the consumer with.
    * @param env environment to manage the consumer lifecycle.
    * @param name the name of the consumer being created.
    * @param f a [[com.datasift.dropwizard.kafka.consumer.StreamProcessor]] to process the decoded
    *          messages.
    * @return a configured and managed [[com.datasift.dropwizard.kafka.consumer.KafkaConsumer]].
    */
  def apply[A : Decoder](conf: KafkaConsumerConfiguration, env: Environment, name: String)
                        (f: StreamProcessor[A]): KafkaConsumer = {
    new KafkaConsumerFactory(env).processWith(implicitly[Decoder[A]], f).build(conf, name)
  }
}
