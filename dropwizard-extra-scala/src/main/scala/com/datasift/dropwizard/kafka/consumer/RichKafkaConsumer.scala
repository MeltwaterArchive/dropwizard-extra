package com.datasift.dropwizard.kafka.consumer

import kafka.serializer.Decoder

/**
 * Enriches a [[com.datasift.dropwizard.kafka.consumer.KafkaConsumer]] with Scala idioms
 *
 * @param consumer the [[com.datasift.dropwizard.kafka.consumer.KafkaConsumer]] to enrich
 */
class RichKafkaConsumer(consumer: KafkaConsumer) {

  /**
   * Consumes a stream of messages of type `A`.
   *
   * Messages will be decoded to type `A` using an implicitly provided
   * [[kafka.serializer.Decoder]].
   *
   * @param processor a [[com.datasift.dropwizard.kafka.consumer.StreamProcessor]] to process the message stream
   * @tparam A the type of the messages in the stream
   */
  def consume[A : Decoder](processor: StreamProcessor[A]) {
    consumer.consume(processor, implicitly[Decoder[A]])
  }
}
