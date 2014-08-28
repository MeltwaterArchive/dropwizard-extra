package com.datasift.dropwizard.kafka;

import io.dropwizard.util.Duration;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

/**
 * Base configuration for Kafka clients.
 *
 * @see com.datasift.dropwizard.kafka.KafkaConsumerFactory
 * @see KafkaProducerFactory
 */
abstract public class KafkaClientFactory {

    @NotNull
    protected Duration socketTimeout = Duration.seconds(30);

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
