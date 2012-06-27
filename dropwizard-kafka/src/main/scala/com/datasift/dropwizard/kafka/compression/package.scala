package com.datasift.dropwizard.kafka

/** Implicits for Kafka compression */
package object compression {

  /** turn it in to a Kafka CompressionCodec that can be used with Kafka's API */
  implicit def toCodec(codec: CompressionCodec): kafka.message.CompressionCodec = {
    kafka.message.CompressionCodec.getCompressionCodec(codec.id)
  }
}
