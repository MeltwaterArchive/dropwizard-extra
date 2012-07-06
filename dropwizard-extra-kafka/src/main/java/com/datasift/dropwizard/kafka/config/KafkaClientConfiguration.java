package com.datasift.dropwizard.kafka.config;

import com.datasift.dropwizard.config.ZooKeeperConfiguration;
import com.yammer.dropwizard.util.Duration;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/**
 * A configuration for a Kafka client.
 */
abstract public class KafkaClientConfiguration {

    @JsonProperty
    @Valid
    @NotNull
    protected ZooKeeperConfiguration zookeeper = new ZooKeeperConfiguration();

    @JsonProperty
    @NotNull
    protected Duration socketTimeout = Duration.seconds(30);

    public ZooKeeperConfiguration getZookeeper() {
        return zookeeper;
    }

    public Duration getSocketTimeout() {
        return socketTimeout;
    }
}
