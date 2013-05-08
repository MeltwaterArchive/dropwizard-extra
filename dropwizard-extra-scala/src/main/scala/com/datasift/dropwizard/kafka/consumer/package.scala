package com.datasift.dropwizard.kafka

import kafka.serializer.{Decoder, StringDecoder, DefaultDecoder}
import java.nio.ByteBuffer
import kafka.message.Message

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

/** Implicit declarations for Kafka Consumers. */
package object consumer {

  /** [[kafka.serializer.Decoder]] for passing through [[kafka.message.Message]]s untouched. */
  implicit val DefaultDecoder: Decoder[Message] = new DefaultDecoder

  /** * [[kafka.serializer.Decoder]] that decodes messages as a [[java.lang.String]]. */
  implicit val StringDecoder: Decoder[String] = new StringDecoder

  /** * [[kafka.serializer.Decoder]] that decodes messages to a raw `byte` array. */
  implicit val BytesDecoder: Decoder[Array[Byte]] = MessageDecoder(message => {
    val bytes = new Array[Byte](message.payload.remaining())
    message.payload.get(bytes)
    bytes
  })

  /** * A [[kafka.serializer.Decoder]] that decodes messages to a raw [[java.nio.ByteBuffer]]. */
  implicit val ByteBufferDecoder: Decoder[ByteBuffer] = MessageDecoder(_.payload)

  /** Implicit for defining a [[com.datasift.dropwizard.kafka.consumer.StreamProcessor]] with a
    * function.
    *
    * @tparam A type of the messages in the stream to process.
    * @param f function to process the stream with.
    * @return [[com.datasift.dropwizard.kafka.consumer.StreamProcessor]] to process the stream.
    */
  implicit def fToStreamProcessorNoTopic[A](f: Iterable[A] => Any): StreamProcessor[A] = {
    new StreamProcessor[A] {
      def process(stream: java.lang.Iterable[A], topic: String) {
        f(stream.asScala)
      }
    }
  }

  /** Implicit for defining a [[com.datasift.dropwizard.kafka.consumer.StreamProcessor]] with a
    * `function`.
    *
    * @tparam A type of the messages in the stream to process.
    * @param f function to process the stream and topic with.
    * @return [[com.datasift.dropwizard.kafka.consumer.StreamProcessor]] to process the stream.
    */
  implicit def fToStreamProcessor[A](f: (Iterable[A], String) => Any): StreamProcessor[A] = {
    new StreamProcessor[A] {
      def process(stream: java.lang.Iterable[A], topic: String) {
        f(stream.asScala, topic)
      }
    }
  }

  /** Implicit for defining a [[com.datasift.dropwizard.kafka.consumer.MessageProcessor]] with a
    * `function`.
    *
    * @tparam A type of the messages in the stream to process.
    * @param f function to process messages with.
    * @return [[com.datasift.dropwizard.kafka.consumer.MessageProcessor]] to process the stream
    */
  implicit def fToMessageProcessorNoTopic[A](f: A => Any): MessageProcessor[A] = {
    new MessageProcessor[A] {
      def process(message: A, topic: String) {
        f(message)
      }
    }
  }

  /** Implicit for defining a [[com.datasift.dropwizard.kafka.consumer.MessageProcessor]] with a
    * `function`.
    *
    * @tparam A type of the messages in the stream to process.
    * @param f function to process messages and topic with.
    * @return a [[com.datasift.dropwizard.kafka.consumer.MessageProcessor]] to process the stream
    */
  implicit def fToMessageProcessor[A](f: (A, String) => Any): MessageProcessor[A] = {
    new MessageProcessor[A] {
      def process(message: A, topic: String) {
        f(message, topic)
      }
    }
  }
}
