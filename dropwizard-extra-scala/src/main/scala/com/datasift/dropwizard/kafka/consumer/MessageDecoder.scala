package com.datasift.dropwizard.kafka.consumer

import kafka.message.Message
import kafka.serializer.Decoder

/**
 * Factory object for [[com.datasift.dropwizard.kafka.consumer.MessageDecoder]]s.
 */
object MessageDecoder {

  /**
   * Creates a [[com.datasift.dropwizard.kafka.consumer.MessageDecoder]] from a function
   *
   * @param f a function that decodes [[kafka.message.Message]]s to type `A`
   * @tparam A the type to decode messages to
   * @return a [[com.datasift.dropwizard.kafka.consumer.MessageDecoder]] for the specified function
   */
  def apply[A](f: Message => A): MessageDecoder[A] = new MessageDecoder[A](f)
}

/**
 * Creates a [[kafka.serializer.Decoder]] from a function.
 *
 * @param f the function to decode [[kafka.message.Message]]s with
 * @tparam A the type to decode messages to
 */
class MessageDecoder[A] private[MessageDecoder] (f: Message => A) extends Decoder[A] {

  /**
   * Decodes the specified [[kafka.message.Message]] as an instance of type `A`.
   *
   * @param message the [[kafka.message.Message]] to decode
   * @return the message as an instance of type `A`
   */
  def toEvent(message: Message): A = f(message)
}
