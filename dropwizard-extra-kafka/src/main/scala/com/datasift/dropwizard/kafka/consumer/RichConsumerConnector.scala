package com.datasift.dropwizard.kafka.consumer

import kafka.consumer.ConsumerConnector
import kafka.serializer.Decoder

/** Provides more idiomatic access to a Consumer's MessageStreams */
class RichConsumerConnector(connector: ConsumerConnector) {

  /** Create message streams for a selection of topics */
  def streams[A : Decoder](topics: Map[String, Int]) = {
    connector.createMessageStreams(topics, implicitly[Decoder[A]])
  }
}
