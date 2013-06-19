package com.datasift.dropwizard.kafka;

import com.datasift.dropwizard.zookeeper.ZooKeeperFactory;
import com.codahale.dropwizard.util.Duration;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/**
 * Base configuration for Kafka clients.
 *
 * @see com.datasift.dropwizard.kafka.config.KafkaConsumerFactory
 * @see KafkaProducerFactory
 */
abstract public class KafkaClientFactory {

    /**
     * The {@link ZooKeeperConfiguration} of the ZooKeeper quorum to use.
     */
    @JsonProperty
    @Valid
    @NotNull
    protected ZooKeeperFactory zookeeper = new ZooKeeperFactory();

    /**
     * The time to wait on a network socket before timing out requests.
     */
    @JsonProperty
    @NotNull
    protected Duration socketTimeout = Duration.seconds(30);

    /**
     * @see KafkaClientFactory#zookeeper
     */
    public ZooKeeperFactory getZookeeper() {
        return zookeeper;
    }

    /**
     * @see KafkaClientFactory#socketTimeout
     */
    public Duration getSocketTimeout() {
        return socketTimeout;
    }
}
