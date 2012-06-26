package com.datasift.dropwizard.kafka.config

import com.yammer.dropwizard.config.Configuration
import reflect.BeanProperty
import javax.validation.constraints.{Min, NotNull}
import com.yammer.dropwizard.util.{Duration, Size}
import com.datasift.dropwizard.kafka.compression.CompressionCodec

/**Configuration for a Kafka producer */
class KafkaProducerConfiguration extends Configuration {

  /** socket send buffer size */
  @BeanProperty
  @NotNull
  val sendBufferSize = Size.kilobytes(10)

  /** maximum time to wait on connection to a broker before failing with an error */
  @BeanProperty
  @NotNull
  val connectionTimeout = Duration.milliseconds(5000)

  /** number of produce requests after which the conection to the broker is refreshed */
  @BeanProperty
  @NotNull
  val reconnectInterval = 30000

  /** maximum size of a message payload */
  @BeanProperty
  @NotNull
  val maxMessageSize = Size.megabytes(1)

  /** compression codec for compressing all messages */
  @BeanProperty
  @NotNull
  val compression: Option[CompressionCodec] = None

  /** optional list of the topics to compress */
  @BeanProperty
  @NotNull
  val compressedTopics: Option[String] = None

  /** number of retries for refreshing the partition cache after a cache miss */
  @BeanProperty
  @Min(0)
  val partitionMissRetries = 3

  /** Configuration for the asynchronous producer, defaults to synchronous */
  @BeanProperty
  @NotNull
  val async: Option[KafkaAsyncProducerConfiguration] = None
}

/** Configuration for the asynchronous Producer */
class KafkaAsyncProducerConfiguration extends Configuration {

  /** maximum time for buffering data in the producer queue */
  @BeanProperty
  @NotNull
  val queueTime = Duration.seconds(5)

  /** maximum size of the queue for buffering data */
  @BeanProperty
  @Min(1)
  val queueSize = 10000

  /** number of messages to batch together before being dispatched */
  @BeanProperty
  @Min(1)
  val batchSize = 200
}
