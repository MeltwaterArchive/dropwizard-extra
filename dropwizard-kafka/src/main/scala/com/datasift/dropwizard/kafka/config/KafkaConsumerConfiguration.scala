package com.datasift.dropwizard.kafka.config

import com.yammer.dropwizard.config.Configuration
import org.hibernate.validator.constraints.NotEmpty
import java.util.Properties
import reflect.BeanProperty
import javax.validation.constraints.{Min, NotNull}
import com.yammer.dropwizard.util.Duration

/**[[com.yammer.dropwizard.config.Configuration]] for a Kafka cluster */
class KafkaConsumerConfiguration extends Configuration {

  // todo: break in to base Kafka-cluster configuration with traits for Producer and Consumer

  /**topic for the consumer to consume */
  @BeanProperty
  @NotEmpty
  val topic: String = ""

  /**consumer group this process belongs to */
  @BeanProperty
  @NotEmpty
  val group: String = ""

  /**number of partitions for this consumer to consume */
  @BeanProperty
  @NotNull
  @Min(1)
  val partitions = 1

  /**number of threads for this consumer to consume with */
  @BeanProperty
  @NotNull
  @Min(1)
  val threads = 1

  /**consumer timeout, in milliseconds */
  @BeanProperty
  @NotNull
  @Min(-1)
  val timeout = -1

  /**consumer socket buffer size, in bytes */
  @BeanProperty
  @NotNull
  @Min(1)
  val receiveBufferSize = 65536

  /**maximum size of each response to a fetch request from a broker */
  @BeanProperty
  @NotNull
  @Min(1)
  val fetchSize = 307200

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
  @Min(0)
  val autoCommitInterval = Duration.seconds(10)

  /**max number of retries during a rebalance */
  @BeanProperty
  @NotNull
  @Min(0)
  val rebalanceRetries = 4

  /**delay before restarting a consumer thread after an error */
  @BeanProperty
  @NotNull
  val restartDelay = Duration.seconds(1)

  /**timeout to wait for the consumer to gracefully shutdown before killing it */
  @BeanProperty
  @NotNull
  val shutdownTimeout = Duration.seconds(10)

  /**Kafka Configuration as a Java Properties object for use by a Consumer */
  def toProperties: Properties = {
    val props = new Properties
    props.put("zk.connect", zookeeper.quorumSpec)
    props.put("zk.connectiontimeout.ms", Long.box(zookeeper.timeout).toString)
    props.put("groupid", group)
    props.put("socket.timeout.ms", Long.box(socketTimeout).toString)
    props.put("socket.buffersize", Long.box(receiveBufferSize).toString)
    props.put("fetch.size", Long.box(fetchSize).toString)
    props.put("backoff.increment.ms", Long.box(backOffIncrement.toMilliseconds).toString)
    props.put("queuedchunks.max", Long.box(queuedChunks).toString)
    props.put("autocommit.enable", Boolean.box(autoCommit).toString)
    props.put("autocommit.interval.ms", Long.box(autoCommitInterval.toMilliseconds).toString)
    props.put("consumer.timeout.ms", Long.box(timeout).toString)
    props.put("rebalance.retries.max", Long.box(rebalanceRetries).toString)
    props
  }

  /**Mapping of topics to number of partitions to consume. */
  def topicMap: Map[String, Int] = Map(topic -> partitions)
}
