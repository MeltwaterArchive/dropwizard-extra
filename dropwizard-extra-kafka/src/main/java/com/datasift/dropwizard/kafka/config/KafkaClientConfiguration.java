package com.datasift.dropwizard.kafka.config;

import com.datasift.dropwizard.zookeeper.config.ZooKeeperConfiguration;
import com.codahale.dropwizard.util.Duration;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/**
 * Base configuration for Kafka clients.
 *
 * @see KafkaConsumerConfiguration
 * @see KafkaProducerConfiguration
 */
abstract public class KafkaClientConfiguration {

    /**
     * The {@link ZooKeeperConfiguration} of the ZooKeeper quorum to use.
     */
    @JsonProperty
    @Valid
    @NotNull
    protected ZooKeeperConfiguration zookeeper = new ZooKeeperConfiguration();

    /**
     * The time to wait on a network socket before timing out requests.
     */
    @JsonProperty
    @NotNull
    protected Duration socketTimeout = Duration.seconds(30);

    /**
     * @see KafkaClientConfiguration#zookeeper
     */
    public ZooKeeperConfiguration getZookeeper() {
        return zookeeper;
    }

    /**
     * @see KafkaClientConfiguration#socketTimeout
     */
    public Duration getSocketTimeout() {
        return socketTimeout;
    }
}
