package com.datasift.dropwizard.kafka.consumer;

import com.yammer.metrics.core.HealthCheck;

/**
 * A {@link HealthCheck} to monitor the health of a {@link KafkaConsumer}.
 *
 * TODO: figure out a way to check the health of a {@link KafkaConsumer} generically
 */
public class KafkaConsumerHealthCheck extends HealthCheck {

    private KafkaConsumer consumer;

    /**
     * Create a new {@link com.yammer.metrics.core.HealthCheck} instance with the given name.
     *
     * @param consumer the {@link KafkaConsumer} to monitor the health of
     * @param name the name of the {@link KafkaConsumer}
     */
    public KafkaConsumerHealthCheck(KafkaConsumer consumer, String name) {
        super(name);
        this.consumer = consumer;
    }

    @Override
    protected Result check() throws Exception {
        return Result.healthy();
    }
}
