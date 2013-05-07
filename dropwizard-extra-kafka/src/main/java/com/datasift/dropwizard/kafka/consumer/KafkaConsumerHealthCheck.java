package com.datasift.dropwizard.kafka.consumer;

import com.codahale.metrics.health.HealthCheck;

/**
 * A {@link HealthCheck} to monitor the health of a {@link KafkaConsumer}.
 */
public class KafkaConsumerHealthCheck extends HealthCheck {

    private final KafkaConsumer consumer;

    /**
     * Create a new {@link HealthCheck} instance with the given name.
     *
     * @param consumer the {@link KafkaConsumer} to monitor the health of.
     */
    public KafkaConsumerHealthCheck(final KafkaConsumer consumer) {
        this.consumer = consumer;
    }

    /**
     * Checks that the {@link KafkaConsumer} is still in its <i>running</i> state.
     *
     * @return true if the {@link KafkaConsumer} is still running properly; false if it is not.
     *
     * @throws Exception if there is an error checking the state of the {@link KafkaConsumer}.
     */
    @Override
    protected Result check() throws Exception {
        return consumer.isRunning()
                ? Result.healthy()
                : Result.unhealthy("Consumer not consuming any partitions");
    }
}
