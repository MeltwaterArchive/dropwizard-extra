package com.datasift.dropwizard.kafka.config

import com.yammer.dropwizard.config.Configuration
import org.hibernate.validator.constraints.NotEmpty
import reflect.BeanProperty
import javax.validation.constraints.{Min, NotNull}
import com.yammer.dropwizard.util.{Size, Duration}
import com.datasift.dropwizard.kafka.consumer.{ErrorPolicy, Shutdown}

/**[[com.yammer.dropwizard.config.Configuration]] for a Kafka cluster */
class KafkaConsumerConfiguration extends Configuration {

  /**consumer group this process belongs to */
  @BeanProperty
  @NotEmpty
  val group: String = ""

  /** mapping of the number of partitions to consume for each topic */
  @BeanProperty
  @NotNull
  val partitions = Map.empty[String, Int]

  /**number of threads for this consumer to consume with */
  @BeanProperty
  @NotNull
  @Min(1)
  val threads = 1

  /**consumer timeout, in milliseconds */
  @BeanProperty
  @NotNull
  val timeout = Duration.seconds(0)

  /**consumer socket buffer size, in bytes */
  @BeanProperty
  @NotNull
  val receiveBufferSize = Size.kilobytes(64)

  /**maximum size of each response to a fetch request from a broker */
  @BeanProperty
  @NotNull
  val fetchSize = Size.kilobytes(300)

  /**milliseconds to back-off polling broker when receiving no data */
  @BeanProperty
  @NotNull
  val backOffIncrement = Duration.seconds(1)

  /**maximum number of chunks to queue in internal buffers */
  @BeanProperty
  @NotNull
  @Min(0)
  val queuedChunks = 100

  /**automatically commit offsets periodically, disable for manual commit */
  @BeanProperty
  @NotNull
  val autoCommit = true

  /**frequency to automatically commit offsets if autocommit is enabled */
  @BeanProperty
  @NotNull
  val autoCommitInterval = Duration.seconds(10)

  /**max number of retries during a rebalance */
  @BeanProperty
  @NotNull
  @Min(0)
  val rebalanceRetries = 4

  /** action to take in the event of an error */
  @BeanProperty
  @NotNull
  val errorPolicy: ErrorPolicy = ErrorPolicy(Shutdown)

  /**delay before restarting a consumer thread after an error */
  @BeanProperty
  @NotNull
  val restartDelay = Duration.seconds(1)
}
