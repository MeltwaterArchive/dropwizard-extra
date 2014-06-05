package com.datasift.dropwizard.kafka;

import com.datasift.dropwizard.zookeeper.ZooKeeperFactory;
import io.dropwizard.Configuration;
import io.dropwizard.util.Duration;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/**
 * Base configuration for Kafka clients.
 *
 * @see com.datasift.dropwizard.kafka.KafkaConsumerFactory
 * @see KafkaProducerFactory
 */
abstract public class KafkaClientFactory {

    @Valid
    @NotNull
    protected ZooKeeperFactory zookeeper = new ZooKeeperFactory();

    @NotNull
    protected Duration socketTimeout = Duration.seconds(30);

    /**
     * Returns the {@link ZooKeeperFactory} of the ZooKeeper quorum to use.
     *
     * @return the ZooKeeper quorum to use.
     */
    @JsonProperty
    public ZooKeeperFactory getZookeeper() {
        return zookeeper;
    }

    /**
     * Sets the {@link ZooKeeperFactory} of the ZooKeeper quorum to use.
     *
     * @param zookeeper the ZooKeeper quorum to use.
     */
    @JsonProperty
    public void setZookeeper(final ZooKeeperFactory zookeeper) {
        this.zookeeper = zookeeper;
    }

    /**
     * Returns the time to wait on a network socket before timing out requests.
     *
     * @return the time to wait on a network socket before timing out requests.
     */
    @JsonProperty
    public Duration getSocketTimeout() {
        return socketTimeout;
    }

    /**
     * Sets the time to wait on a network socket before timing out requests.
     *
     * @param socketTimeout the time to wait on a network socket before timing out requests.
     */
    @JsonProperty
    public void setSocketTimeout(final Duration socketTimeout) {
        this.socketTimeout = socketTimeout;
    }
}
