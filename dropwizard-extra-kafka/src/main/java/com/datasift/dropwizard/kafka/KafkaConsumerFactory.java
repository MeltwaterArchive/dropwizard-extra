package com.datasift.dropwizard.kafka;

import com.datasift.dropwizard.kafka.config.KafkaConsumerConfiguration;
import com.datasift.dropwizard.kafka.consumer.KafkaConsumer;
import com.datasift.dropwizard.kafka.consumer.KafkaConsumerHealthCheck;
import com.datasift.dropwizard.kafka.consumer.ThreadPooledConsumer;
import com.yammer.dropwizard.config.Environment;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * A factory for creating and managing {@link KafkaConsumer} instances.
 *
 * The {@link KafkaConsumer} implementation will be determined by the configuration
 * used to create it.
 *
 * The resultant {@link KafkaConsumer} will have its lifecycle managed by the
 * {@link Environment} and will have {@link com.yammer.metrics.core.HealthCheck}s
 * installed to monitor its status.
 */
public class KafkaConsumerFactory {

    private Environment environment;

    /**
     * Creates a new {@link KafkaConsumerFactory} instance for the specified {@link Environment}.
     *
     * @param environment the {@link Environment} to build {@link KafkaConsumer} instances for
     */
    public KafkaConsumerFactory(Environment environment) {
        this.environment = environment;
    }

    /**
     * Builds a default {@link KafkaConsumer} instance from the specified {@link KafkaConsumerConfiguration}.
     *
     * @param configuration the {@link KafkaConsumerConfiguration} instance to configure the {@link KafkaConsumer} with
     * @return a {@link KafkaConsumer}, managed and configured according to the {@code configuration}
     */
    public KafkaConsumer build(KafkaConsumerConfiguration configuration) {
        return build(configuration, "default");
    }

    /**
     * Builds a {@link KafkaConsumer} instance from the specified {@link KafkaConsumerConfiguration} with the given name.
     *
     * @param configuration the {@link KafkaConsumerConfiguration} instance to configure the {@link KafkaConsumer} with
     * @param name a name for the {@link KafkaConsumer} instance
     * @return a {@link KafkaConsumer}, managed and configured according to the {@code configuration}
     */
    public KafkaConsumer build(KafkaConsumerConfiguration configuration, String name) {

        KafkaConsumer consumer = new ThreadPooledConsumer(
                Consumer.createJavaConsumerConnector(toConsumerConfig(configuration)),
                configuration.getPartitions(),
                configuration.getErrorPolicy(),
                configuration.getShutdownPeriod(),
                name);

        // manage the consumer
        environment.manage(consumer);

        // add health checks
        environment.addHealthCheck(new KafkaConsumerHealthCheck(consumer, name));

        return consumer;
    }

    private static ConsumerConfig toConsumerConfig(KafkaConsumerConfiguration configuration) {
        Properties props = new Properties();
        props.put("zk.connect", configuration.getZookeeper().getQuorumSpec());
        props.put("zk.connectiontimeout.ms", configuration.getZookeeper().getTimeout().toMilliseconds());
        props.put("groupid", configuration.getGroup());
        props.put("socket.timeout.ms", configuration.getSocketTimeout().toMilliseconds());
        props.put("socket.buffersize", configuration.getReceiveBufferSize().toBytes());
        props.put("fetch.size", configuration.getFetchSize().toBytes());
        props.put("backoff.increment.ms", configuration.getBackOffIncrement().toMilliseconds());
        props.put("queuedchunks.max", configuration.getQueuedChunks());
        props.put("autocommit.enable", configuration.getAutoCommit());
        props.put("autocommit.interval.ms", configuration.getAutoCommitInterval().toMilliseconds());
        props.put("consumer.timeout.ms", configuration.getTimeout().toMilliseconds());
        props.put("rebalance.retries.max", configuration.getRebalanceRetries());
        return new ConsumerConfig(props);
    }
}
