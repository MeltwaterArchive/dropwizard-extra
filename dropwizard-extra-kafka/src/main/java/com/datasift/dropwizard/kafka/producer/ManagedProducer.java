package com.datasift.dropwizard.kafka.producer;

import io.dropwizard.lifecycle.Managed;
import kafka.javaapi.producer.Producer;

/**
 * Manages a Kafka {@link Producer} as part of the application lifecycle..
 */
public class ManagedProducer implements Managed {

    private final Producer<?, ?> producer;

    public ManagedProducer(final Producer<?, ?> producer) {
        this.producer = producer;
    }

    @Override
    public void start() throws Exception {
        // nothing to do, already started
    }

    @Override
    public void stop() throws Exception {
        producer.close();
    }
}
