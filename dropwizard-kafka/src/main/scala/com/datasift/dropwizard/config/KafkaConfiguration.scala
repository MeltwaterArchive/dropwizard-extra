package com.datasift.dropwizard.config

import com.yammer.dropwizard.config.Configuration
import javax.validation.Valid
import org.hibernate.validator.constraints.NotEmpty
import java.util.Properties
import reflect.BeanProperty
import javax.validation.constraints.{Min, NotNull}

/** [[com.yammer.dropwizard.config.Configuration]] for a Kafka cluster */
class KafkaConfiguration extends Configuration {

  // todo: break in to base Kafka-cluster configuration with traits for Producer and Consumer
  // todo: move to dropwizard-kafka

  /** topic for the consumer to consume */
  @BeanProperty
  @NotEmpty
  val topic: String = ""

  /** consumer group this process belongs to */
  @BeanProperty
  @NotEmpty
  val group: String = ""

  /** number of partitions for this consumer to consume */
  @BeanProperty
  @NotNull
  @Min(1)
  val partitions = 1

  /** number of threads for this consumer to consume with */
  @BeanProperty
  @NotNull
  @Min(1)
  val threads = 1

  /** consumer timeout, in milliseconds */
  @BeanProperty
  @NotNull
  @Min(-1)
  val timeout = -1

  /** consumer socket timeout, in milliseconds */
  @BeanProperty
  @NotNull
  @Min(0)
  val socketTimeout = 30000

  /** consumer socket buffer size, in bytes */
  @BeanProperty
  @NotNull
  @Min(1)
  val bufferSize = 65536

  /** maximum size of each response to a fetch request from a broker */
  @BeanProperty
  @NotNull
  @Min(1)
  val fetchSize = 307200

  /** milliseconds to back-off polling broker when receiving no data */
  @BeanProperty
  @NotNull
  @Min(0)
  val backoffIncrement = 1000

  /** maximum number of chunks to queue in internal buffers */
  @BeanProperty
  @NotNull
  @Min(0)
  val queuedChunks = 100

  /** automatically commit offsets periodically, disable for manual commit */
  @BeanProperty
  @NotNull
  val autocommit = true

  /** frequency to automatically commit offsets if autocommit is enabled */
  @BeanProperty
  @NotNull
  @Min(0)
  val autocommitInterval = 10000

  /** max number of retries during a rebalance */
  @BeanProperty
  @NotNull
  @Min(0)
  val rebalanceRetries = 4

  /** configuration for the cluster's ZooKeeper quroum */
  @BeanProperty
  @NotNull
  @Valid
  val zookeeper = new ZooKeeperConfiguration

  /** Kafka Configuration as a Java Properties object for use by a Consumer */
  def toProperties: Properties = {
    val props = new Properties
    props.put("zk.connect", zookeeper.quorumSpec)
    props.put("zk.connectiontimeout.ms", Long.box(zookeeper.timeout))
    props.put("groupid", group)
    props.put("socket.timeout.ms", Long.box(socketTimeout))
    props.put("socket.buffersize", Long.box(bufferSize))
    props.put("fetch.size", Long.box(fetchSize))
    props.put("backoff.increment.ms", Long.box(backoffIncrement))
    props.put("queuedchunks.max", Long.box(queuedChunks))
    props.put("autocommit.enable", Boolean.box(autocommit))
    props.put("autocommit.interval.ms", Long.box(autocommitInterval))
    props.put("consumer.timeout.ms", Long.box(timeout))
    props.put("rebalance.retries.max", Long.box(rebalanceRetries))
    props
  }

  /** Mapping of topics to number of partitions to consume. */
  def topicMap: Map[String, Int] = Map(topic -> partitions)
}
