package com.datasift.dropwizard.kafka.consumer

import kafka.message.Message
import kafka.serializer.Decoder

/** Utility for creating Kafka Message Decoders from a function */
object MessageDecoder {

  def apply[A](f: Message => A): MessageDecoder[A] = new MessageDecoder(f)
}

/** Kafka Message MessageDecoder configured to decode a particular type */
class MessageDecoder[A](f: Message => A) extends Decoder[A] {

  /** decodes a message */
  def toEvent(message: Message): A = {
    f(message)
  }
}
