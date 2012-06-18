package com.datasift.dropwizard.kafka

import kafka.consumer.ConsumerConnector
import kafka.serializer.{Decoder, StringDecoder, DefaultDecoder}
import java.nio.ByteBuffer

/** Implicits for the Kafka Consumer */
package object consumer {

  /** Pass-thru Decoder for Messages */
  implicit object DefaultDecoder extends DefaultDecoder

  /** Decoder for UTF-8 Strings */
  implicit object StringDecoder extends StringDecoder

  /** Decoder for raw byte streams */
  implicit val BytesDecoder: Decoder[Array[Byte]] =
    MessageDecoder(message => {
      val bytes = new Array[Byte](message.payload.remaining)
      message.payload.get(bytes)
      bytes
    })

  /** Decoder for the message's [[java.nio.ByteBuffer]] */
  implicit val ByteBufferDecoder: Decoder[ByteBuffer] = MessageDecoder(_.payload)

  implicit def enrichConsumerConnector(connector: ConsumerConnector) = {
    new RichConsumerConnector(connector)
  }

  /** Provides more idiomatic access to a Consumer's MessageStreams */
  class RichConsumerConnector(connector: ConsumerConnector) {

    /** Create message streams for a selection of topics */
    def streams[A : Decoder](topics: Map[String, Int]) = {
      connector.createMessageStreams[A](topics, implicitly[Decoder[A]])
    }

    /** Create message streams for a topic */
    def streams[A : Decoder](topic: String, partitions: Int) = {
      connector.createMessageStreams[A](
        Map(topic -> partitions),
        implicitly[Decoder[A]]
      ).apply(topic)
    }
  }
}
