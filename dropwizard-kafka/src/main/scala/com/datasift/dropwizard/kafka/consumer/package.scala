package com.datasift.dropwizard.kafka

import kafka.consumer.ConsumerConnector
import kafka.serializer.{Decoder => KDecoder, StringDecoder, DefaultDecoder}

/** Implicits for the Kafka Consumer */
package object consumer {

  /** Pass-thru Decoder for Messages */
  implicit object DefaultDecoder extends DefaultDecoder

  /** Decoder for UTF-8 Strings */
  implicit object StringDecoder extends StringDecoder

  /** Decoder for raw byte streams */
  implicit val BytesDecoder: KDecoder[Array[Byte]] =
    Decoder(message => {
      val bytes = new Array[Byte](message.payload.remaining)
      message.payload.get(bytes)
      bytes
    })

  implicit def enrichConsumerConnector(connector: ConsumerConnector) = {
    new RichConsumerConnector(connector)
  }

  /** Provides more idiomatic access to a Consumer's MessageStreams */
  class RichConsumerConnector(connector: ConsumerConnector) {

    /** Create message streams for a selection of topics */
    def streams[A : KDecoder](topics: Map[String, Int]) = {
      connector.createMessageStreams[A](topics, implicitly[KDecoder[A]])
    }

    /** Create message streams for a topic */
    def streams[A : KDecoder](topic: String, partitions: Int) = {
      connector.createMessageStreams[A](
        Map(topic -> partitions),
        implicitly[KDecoder[A]]
      ).apply(topic)
    }
  }
}
