package com.datasift.dropwizard.kafka.consumer

import com.datasift.dropwizard.conf.KafkaConfiguration
import com.yammer.dropwizard.lifecycle.Managed
import com.yammer.dropwizard.Logging
import kafka.consumer.{KafkaMessageStream, ConsumerConnector}
import java.util.concurrent.{TimeUnit, Executors}

/**A Kafka Consumer run using a fixed ThreadPool
 *
 * When a consuming stream fails due to an uncaught Exception, the stream
 * consumer is restarted after `restartDelay` milliseconds.
 *
 * @param conf KafkaConfiguration that configures this consumer
 * @param restartDelay delay in milliseconds before restarting a stream consumer
 * @param f the function each Thread should run to process a KafkaMessageStream
 */
class ThreadPooledConsumer[A : kafka.serializer.Decoder]
(conf: KafkaConfiguration, restartDelay: Long, f: KafkaMessageStream[A] => Unit)
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
                    "Uncaught {} while processing stream, restarting in {} {}",
                    t.getClass.getSimpleName,
                    restartDelay.toString,
                    TimeUnit.MILLISECONDS.toString.toLowerCase
                  )
                  executor.schedule(this, restartDelay, TimeUnit.MILLISECONDS)
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
    executor.awaitTermination(10, TimeUnit.SECONDS)

    // commit the offsets and shutdown the consumer
    consumer foreach {
      consumer =>
        consumer.commitOffsets
        consumer.shutdown()
    }
    log.info("Stopped Consumer")
  }
}
