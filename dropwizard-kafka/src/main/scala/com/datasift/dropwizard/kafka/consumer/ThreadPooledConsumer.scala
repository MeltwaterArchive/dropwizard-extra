package com.datasift.dropwizard.kafka.consumer

import com.datasift.dropwizard.kafka.config.KafkaConsumerConfiguration
import com.yammer.dropwizard.lifecycle.Managed
import com.yammer.dropwizard.Logging
import kafka.consumer.{KafkaMessageStream, ConsumerConnector}
import java.util.concurrent.{TimeUnit, Executors}
import kafka.serializer.Decoder
import com.yammer.dropwizard.config.Environment

object ThreadPooledConsumer {

  /** create and manage a Kafka Consumer using a Thread Pool
   *
   * @param conf consumer configuration
   * @param f function to process each [[kafka.consumer.KafkaMessageStream]]
   * @tparam A type of the messages being processed, must have an implicit
   *           [[kafka.serializer.Decoder]] in scope
   * @return a consumer configured to process messages using a Thread Pool
   */
  def apply[A : Decoder](conf: KafkaConsumerConfiguration, env: Environment)
                        (f: KafkaMessageStream[A] => Unit): ThreadPooledConsumer[A] = {
    val consumer = new ThreadPooledConsumer[A](conf, f)
    env.manage(consumer)
    consumer
  }
}

/** Kafka Consumer run using a fixed ThreadPool
 *
 * When a consuming stream fails due to an uncaught Exception, the stream
 * consumer is restarted after `restartDelay` milliseconds.
 *
 * @param conf KafkaConsumerConfiguration that configures this consumer
 * @param f the function each Thread should run to process a KafkaMessageStream
 */
class ThreadPooledConsumer[A : Decoder](conf: KafkaConsumerConfiguration,
                                        f: KafkaMessageStream[A] => Unit)
  extends Managed with Logging {

  var consumer: Option[ConsumerConnector] = None
  val executor = Executors.newScheduledThreadPool(conf.threads)

  def start() {
    consumer = Option(Consumer(conf))
    consumer foreach {
      consumer =>
        val streams = consumer.streams[A](conf.topic, conf.partitions)

        for (stream <- streams) {
          executor.submit(new Runnable {
            def run() {
              try {
                // consume stream
                f(stream)
              } catch {
                case _: InterruptedException => {
                  log.info("Consumer stream interrupted, shutting down")
                }
                case t: Throwable => {
                  log.error(t,
                    "Uncaught {} while processing stream, restarting in {}ms",
                    t.getClass.getSimpleName,
                    conf.restartDelay.toString
                  )
                  executor.schedule(this, conf.restartDelay.toNanoseconds, TimeUnit.NANOSECONDS)
                }
              }
            }
          })
        }

        log.info("Consumer started for {} partitions of {} using {} threads",
          conf.partitions.toString,
          conf.topic,
          conf.threads.toString)
    }
  }

  def stop() {
    log.info("Stopping Consumer")
    // first shutdown consumer threads
    executor.shutdownNow()
    executor.awaitTermination(conf.shutdownTimeout.toNanoseconds, TimeUnit.NANOSECONDS)

    // commit the offsets and shutdown the consumer
    consumer.foreach(_.shutdown())
    log.info("Stopped Consumer")
  }
}
