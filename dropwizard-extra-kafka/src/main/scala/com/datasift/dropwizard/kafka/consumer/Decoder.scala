package com.datasift.dropwizard.kafka.consumer

import kafka.message.Message

/** Utility for creating Kafka Message Decoders from a function */
object Decoder {

  def apply[A](f: Message => A): Decoder[A] = new Decoder(f)
}

/** Kafka Message Decoder configured to decode a particular type */
class Decoder[A](f: Message => A) extends kafka.serializer.Decoder[A] {

  /** decodes a message */
  def toEvent(message: Message): A = {
    f(message)
  }
}
