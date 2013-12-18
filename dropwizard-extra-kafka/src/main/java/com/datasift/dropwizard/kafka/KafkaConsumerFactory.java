package com.datasift.dropwizard.kafka;

import com.datasift.dropwizard.zookeeper.config.ZooKeeperConfiguration;
import com.datasift.dropwizard.kafka.config.KafkaConsumerConfiguration;
import com.datasift.dropwizard.kafka.consumer.KafkaConsumer;
import com.datasift.dropwizard.kafka.consumer.KafkaConsumerHealthCheck;
import com.datasift.dropwizard.kafka.consumer.StreamProcessor;
import com.datasift.dropwizard.kafka.consumer.SynchronousConsumer;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.lifecycle.ServerLifecycleListener;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.message.Message;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import org.eclipse.jetty.server.Server;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A factory for creating and managing {@link KafkaConsumer} instances.
 * <p/>
 * The {@link KafkaConsumer} implementation will be determined by the configuration used to create
 * it.
 * <p/>
 * The resultant {@link KafkaConsumer} will have its lifecycle managed by the {@link Environment}
 * and will have {@link com.yammer.metrics.core.HealthCheck}s installed to monitor its status.
 */
public class KafkaConsumerFactory {

    private final Environment environment;

    /**
     * Creates a new {@link KafkaConsumerFactory} instance for the specified {@link Environment}.
     *
     * @param environment the {@link Environment} to build {@link KafkaConsumer} instances for.
     */
    public KafkaConsumerFactory(final Environment environment) {
        this.environment = environment;
    }

    /**
     * Prepares a {@link KafkaConsumerBuilder} for a given {@link StreamProcessor}.
     *
     * @param processor the {@link StreamProcessor} to process the stream with.
     * @return a {@link KafkaConsumerBuilder} to build a {@link KafkaConsumer} for the given
     *         processor.
     */
    public KafkaConsumerBuilder<Message> processWith(final StreamProcessor<Message> processor) {
        return processWith(new DefaultDecoder(), processor);
    }

    /**
     * Prepares a {@link KafkaConsumerBuilder} for a given {@link Decoder} and {@link
     * StreamProcessor}.
     * <p/>
     * The decoder instance is used to decode {@link Message}s in the stream before being passed to
     * the processor.
     *
     * @param decoder the {@link Decoder} instance to decode messages with
     * @param processor a {@link StreamProcessor} to process the message stream
     * @return a {@link KafkaConsumerBuilder} to build a {@link KafkaConsumer} for the given
     *         processor and decoder.
     */
    public <T> KafkaConsumerBuilder<T> processWith(final Decoder<T> decoder,
                                                   final StreamProcessor<T> processor) {
        return new KafkaConsumerBuilder<T>(decoder, processor);
    }

    /**
     * A Builder for building a configured {@link KafkaConsumer}.
     *
     * @param <T> the type of the messages the {@link KafkaConsumer} will process.
     */
    public class KafkaConsumerBuilder<T> {

        private final Decoder<T> decoder;
        private final StreamProcessor<T> processor;

        private KafkaConsumerBuilder(final Decoder<T> decoder, final StreamProcessor<T> processor) {
            this.decoder = decoder;
            this.processor = processor;
        }

        /**
         * Builds a {@link KafkaConsumer} instance from the given {@link KafkaConsumerConfiguration}.
         *
         * @param configuration the {@link KafkaConsumerConfiguration} to configure the {@link
         *                      KafkaConsumer} with.
         *
         * @return a managed and configured {@link KafkaConsumer}.
         */
        public KafkaConsumer build(final KafkaConsumerConfiguration configuration) {
            return build(configuration, "default");
        }

        /**
         * Builds a {@link KafkaConsumer} instance from the given {@link
         * KafkaConsumerConfiguration}, {@link ExecutorService} and name.
         * <p/>
         * The name is used to identify the returned {@link KafkaConsumer} instance, for example, as
         * the name of its {@link com.yammer.metrics.core.HealthCheck}s, thread pool, etc.
         * <p/>
         * This implementation creates a new {@link ExecutorService} with a fixed-size thread-pool,
         * configured for one thread per-partition the {@link KafkaConsumer} is being configured to
         * consume.
         *
         * @param configuration the {@link KafkaConsumerConfiguration} to configure the {@link
         *                      KafkaConsumer} with.
         * @param name the name of the {@link KafkaConsumer}.
         *
         * @return a managed and configured {@link KafkaConsumer}.
         */
        public KafkaConsumer build(final KafkaConsumerConfiguration configuration,
                                   final String name) {

            int threads = 0;
            for (final Integer p : configuration.getPartitions().values()) {
                threads = threads + p;
            }

            final ExecutorService executor = environment.managedExecutorService(
                    String.format("kafka-consumer-%s-%%d", name),
                    threads, threads, 0, TimeUnit.SECONDS);

            return build(configuration, executor, name);
        }

        /**
         * Builds a {@link KafkaConsumer} instance from the given {@link
         * KafkaConsumerConfiguration}, {@link ExecutorService} and name.
         * <p/>
         * The name is used to identify the returned {@link KafkaConsumer} instance, for example, as
         * the name of its {@link com.yammer.metrics.core.HealthCheck}s, etc.
         *
         * @param configuration the {@link KafkaConsumerConfiguration} to configure the {@link
         *                      KafkaConsumer} with.
         * @param executor the {@link ExecutorService} to process messages with.
         * @param name the name of the {@link KafkaConsumer}.
         *
         * @return a managed and configured {@link KafkaConsumer}.
         */
        public KafkaConsumer build(final KafkaConsumerConfiguration configuration,
                                   final ExecutorService executor,
                                   final String name) {

            final SynchronousConsumer consumer = new SynchronousConsumer<T>(
                    Consumer.createJavaConsumerConnector(toConsumerConfig(configuration)),
                    configuration.getPartitions(),
                    decoder,
                    processor,
                    executor,
                    configuration.getInitialRecoveryDelay(),
                    configuration.getMaxRecoveryDelay(),
                    configuration.getRetryResetDelay(),
                    configuration.getMaxRecoveryAttempts(),
                    configuration.isShutdownOnFatal());

            //provide a reference to the Jetty server to the consumer
            environment.addServerLifecycleListener(new ServerLifecycleListener() {
                @Override
                public void serverStarted(Server server) {
                    consumer.setServer(server);
                }
            });

            // manage the consumer
            environment.manage(consumer);

            // add health checks
            environment.addHealthCheck(new KafkaConsumerHealthCheck(consumer, name));

            return consumer;
        }
    }

    static ConsumerConfig toConsumerConfig(final KafkaConsumerConfiguration configuration) {
        final ZooKeeperConfiguration zookeeper = configuration.getZookeeper();
        final Properties props = new Properties();

        props.setProperty("zk.connect",
                zookeeper.getQuorumSpec());
        props.setProperty("zk.connectiontimeout.ms",
                String.valueOf(zookeeper.getConnectionTimeout().toMilliseconds()));
        props.setProperty("zk.sessiontimeout.ms",
                String.valueOf(zookeeper.getSessionTimeout().toMilliseconds()));
        props.setProperty("groupid",
                configuration.getGroup());
        props.setProperty("socket.timeout.ms",
                String.valueOf(configuration.getSocketTimeout().toMilliseconds()));
        props.setProperty("socket.buffersize",
                String.valueOf(configuration.getReceiveBufferSize().toBytes()));
        props.setProperty("fetch.size",
                String.valueOf(configuration.getFetchSize().toBytes()));
        props.setProperty("backoff.increment.ms",
                String.valueOf(configuration.getBackOffIncrement().toMilliseconds()));
        props.setProperty("queuedchunks.max",
                String.valueOf(configuration.getQueuedChunks()));
        props.setProperty("autocommit.enable",
                String.valueOf(configuration.getAutoCommit()));
        props.setProperty("autocommit.interval.ms",
                String.valueOf(configuration.getAutoCommitInterval().toMilliseconds()));
        props.setProperty("consumer.timeout.ms",
                String.valueOf(configuration.getTimeout().toMilliseconds()));
        props.setProperty("rebalance.retries.max",
                String.valueOf(configuration.getRebalanceRetries()));

        return new ConsumerConfig(props);
    }
}
