package com.datasift.dropwizard.kafka

import kafka.serializer.{Decoder, StringDecoder, DefaultDecoder}
import java.nio.ByteBuffer
import kafka.consumer.KafkaMessageStream

/**
 * Implicit declarations for Kafka Consumers.
 */
package object consumer {

  /**
   * A [[kafka.serializer.Decoder]] for passing through [[kafka.message.Message]]s untouched
   */
  implicit object DefaultDecoder extends DefaultDecoder

  /**
   * A [[kafka.serializer.Decoder]] that decodes messages as a [[java.lang.String]].
   */
  implicit object StringDecoder extends StringDecoder

  /**
   * A [[kafka.serializer.Decoder]] that decodes messages to a raw `byte` array.
   */
  implicit val BytesDecoder: Decoder[Array[Byte]] = MessageDecoder(message => {
    val bytes = new Array[Byte](message.payload.remaining())
    message.payload.get(bytes)
    bytes
  })

  /**
   * A [[kafka.serializer.Decoder]] that decodes messages to a raw [[java.nio.ByteBuffer]].
   */
  implicit val ByteBufferDecoder: Decoder[ByteBuffer] = MessageDecoder(_.payload)

  /**
   * Implicit for defining a [[com.datasift.dropwizard.kafka.consumer.StreamProcessor]] with a `function`.
   *
   * @param f the function to process the stream with
   * @tparam A the type of the messages in the stream to process
   * @return a [[com.datasift.dropwizard.kafka.consumer.StreamProcessor]] for processing the stream
   */
  implicit def fToStreamProcessorNoTopic[A](f: KafkaMessageStream[A] => Unit): StreamProcessor[A] = {
    new StreamProcessor[A] {
      def process(stream: KafkaMessageStream[A], topic: String) {
        f(stream)
      }
    }
  }

  /**
   * Implicit for defining a [[com.datasift.dropwizard.kafka.consumer.StreamProcessor]] with a `function`.
   *
   * @param f the function to process the stream with
   * @tparam A the type of the messages in the stream to process
   * @return a [[com.datasift.dropwizard.kafka.consumer.StreamProcessor]] for processing the stream
   */
  implicit def fToStreamProcessor[A](f: (KafkaMessageStream[A], String) => Unit): StreamProcessor[A] = {
    new StreamProcessor[A] {
      def process(stream: KafkaMessageStream[A], topic: String) {
        f(stream, topic)
      }
    }
  }

  /**
   * Implicit for defining a [[com.datasift.dropwizard.kafka.consumer.MessageProcessor]] with a `function`.
   *
   * @param f the function to process messages with
   * @tparam A the type of the messages in the stream to process
   * @return a [[com.datasift.dropwizard.kafka.consumer.MessageProcessor]] for processing messages in the stream
   */
  implicit def fToMessageProcessorNoTopic[A](f: A => Unit): MessageProcessor[A] = {
    new MessageProcessor[A] {
      def process(message: A, topic: String) {
        f(message)
      }
    }
  }

  /**
   * Implicit for defining a [[com.datasift.dropwizard.kafka.consumer.MessageProcessor]] with a `function`.
   *
   * @param f the function to process messages with
   * @tparam A the type of the messages in the stream to process
   * @return a [[com.datasift.dropwizard.kafka.consumer.MessageProcessor]] for processing messages in the stream
   */
  implicit def fToMessageProcessor[A](f: (A, String) => Unit): MessageProcessor[A] = {
    new MessageProcessor[A] {
      def process(message: A, topic: String) {
        f(message, topic)
      }
    }
  }
}
