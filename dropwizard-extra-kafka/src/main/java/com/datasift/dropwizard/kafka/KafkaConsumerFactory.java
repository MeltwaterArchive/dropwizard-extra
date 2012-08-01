package com.datasift.dropwizard.kafka;

import com.datasift.dropwizard.kafka.config.KafkaConsumerConfiguration;
import com.datasift.dropwizard.kafka.consumer.KafkaConsumer;
import com.datasift.dropwizard.kafka.consumer.KafkaConsumerHealthCheck;
import com.datasift.dropwizard.kafka.consumer.StreamProcessor;
import com.datasift.dropwizard.kafka.consumer.ThreadPooledConsumer;
import com.yammer.dropwizard.config.Environment;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.message.Message;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;

import java.util.Properties;

/**
 * A factory for creating and managing {@link KafkaConsumer} instances.
 * <p>
 * The {@link KafkaConsumer} implementation will be determined by the
 * configuration used to create it.
 * <p>
 * The resultant {@link KafkaConsumer} will have its lifecycle managed by the
 * {@link Environment} and will have {@link com.yammer.metrics.core.HealthCheck}s
 * installed to monitor its status.
 */
public class KafkaConsumerFactory {

    private Environment environment;

    /**
     * Creates a new {@link KafkaConsumerFactory} instance for the specified
     * {@link Environment}.
     *
     * @param environment the {@link Environment} to build {@link KafkaConsumer}
     *                    instances for
     */
    public KafkaConsumerFactory(Environment environment) {
        this.environment = environment;
    }

    /**
     * Prepares a {@link KafkaConsumerBuilder} for a given
     * {@link StreamProcessor}.
     *
     * @param processor the {@link StreamProcessor} to process the stream with
     * @return          a {@link KafkaConsumerBuilder} to build a
     *                  {@link KafkaConsumer} for the given processor
     */
    public KafkaConsumerBuilder<Message> processWith(StreamProcessor<Message> processor) {
        return processWith(new DefaultDecoder(), processor);
    }

    /**
     * Prepares a {@link KafkaConsumerBuilder} for a given {@link Decoder} and
     * {@link StreamProcessor}.
     * <p>
     * The decoder instance is used to decode {@link Message}s in the stream
     * before being passed to the processor.
     *
     * @param decoder   the {@link Decoder} instance to decode messages with
     * @param processor a {@link StreamProcessor} to process the message stream
     * @return          a {@link KafkaConsumerBuilder} to build a
     *                  {@link KafkaConsumer} for the given processor and decoder
     */
    public <T> KafkaConsumerBuilder<T> processWith(Decoder<T> decoder,
                                                   StreamProcessor<T> processor) {
        return new KafkaConsumerBuilder<T>(decoder, processor);
    }

    /**
     * A Builder for building a configured {@link KafkaConsumer}.
     *
     * @param <T> the type of the messages the {@link KafkaConsumer} will process
     */
    public class KafkaConsumerBuilder<T> {

        private Decoder<T> decoder;
        private StreamProcessor<T> processor;

        private KafkaConsumerBuilder(Decoder<T> decoder,
                                     StreamProcessor<T> processor) {
            this.decoder = decoder;
            this.processor = processor;
        }

        /**
         * Builds a {@link KafkaConsumer} instance from the given
         * {@link KafkaConsumerConfiguration}.
         *
         * @param configuration the {@link KafkaConsumerConfiguration}
         *                      to configure the {@link KafkaConsumer} with
         * @return              a managed and configured {@link KafkaConsumer}
         */
        public KafkaConsumer<T> build(KafkaConsumerConfiguration configuration) {
            return build(configuration, "default");
        }

        /**
         * Builds a {@link KafkaConsumer} instance from the given
         * {@link KafkaConsumerConfiguration} and name.
         * <p>
         * The name is used to identify the returned {@link KafkaConsumer}
         * instance, for example, as the name of its
         * {@link com.yammer.metrics.core.HealthCheck}s, thread pool, etc.
         *
         * @param configuration the {@link KafkaConsumerConfiguration}
         *                      to configure the {@link KafkaConsumer} with
         * @param name          the name of the {@link KafkaConsumer}
         * @return              a managed and configured {@link KafkaConsumer}
         */
        public KafkaConsumer<T> build(KafkaConsumerConfiguration configuration,
                                      String name) {
            KafkaConsumer<T> consumer = new ThreadPooledConsumer<T>(
                    Consumer.createJavaConsumerConnector(toConsumerConfig(configuration)),
                    configuration.getPartitions(),
                    configuration.getShutdownPeriod(),
                    decoder,
                    processor,
                    name);

            // manage the consumer
            environment.manage(consumer);

            // add health checks
            environment.addHealthCheck(new KafkaConsumerHealthCheck(consumer, name));

            return consumer;
        }
    }

    private static ConsumerConfig toConsumerConfig(KafkaConsumerConfiguration configuration) {
        Properties props = new Properties();
        props.put("zk.connect",
                configuration.getZookeeper().getQuorumSpec());
        props.put("zk.connectiontimeout.ms",
                configuration.getZookeeper().getConnectionTimeout().toMilliseconds());
        props.put("zk.sessiontimeout.ms",
                configuration.getZookeeper().getSessionTimeout().toMilliseconds());
        props.put("groupid",
                configuration.getGroup());
        props.put("socket.timeout.ms",
                configuration.getSocketTimeout().toMilliseconds());
        props.put("socket.buffersize",
                configuration.getReceiveBufferSize().toBytes());
        props.put("fetch.size",
                configuration.getFetchSize().toBytes());
        props.put("backoff.increment.ms",
                configuration.getBackOffIncrement().toMilliseconds());
        props.put("queuedchunks.max",
                configuration.getQueuedChunks());
        props.put("autocommit.enable",
                configuration.getAutoCommit());
        props.put("autocommit.interval.ms",
                configuration.getAutoCommitInterval().toMilliseconds());
        props.put("consumer.timeout.ms",
                configuration.getTimeout().toMilliseconds());
        props.put("rebalance.retries.max",
                configuration.getRebalanceRetries());
        return new ConsumerConfig(props);
    }
}
