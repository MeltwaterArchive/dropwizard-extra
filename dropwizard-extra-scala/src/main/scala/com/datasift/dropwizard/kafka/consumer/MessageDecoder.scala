package com.datasift.dropwizard.kafka.consumer

import kafka.message.Message
import kafka.serializer.Decoder

/** Factory object for [[com.datasift.dropwizard.kafka.consumer.MessageDecoder]]s. */
object MessageDecoder {

  /** Creates a [[com.datasift.dropwizard.kafka.consumer.MessageDecoder]] from a function.
    *
    * @tparam A the type to decode messages to.
    * @param f a function that decodes [[kafka.message.Message]]s to type `A`.
    * @return a [[com.datasift.dropwizard.kafka.consumer.MessageDecoder]] for the specified function.
    */
  def apply[A](f: Message => A): MessageDecoder[A] = new MessageDecoder[A](f)
}

/** Creates a [[kafka.serializer.Decoder]] from a function.
  *
  * @tparam A type to decode messages to.
  * @param f function to decode [[kafka.message.Message]]s with.
  */
class MessageDecoder[A] private[MessageDecoder](f: Message => A) extends Decoder[A] {

  /** Decodes the specified [[kafka.message.Message]] as an instance of type `A`.
    *
    * @param message [[kafka.message.Message]] to decode.
    * @return the message as an instance of type `A`.
    */
  def toEvent(message: Message): A = f(message)
}
