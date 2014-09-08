package com.datasift.dropwizard.kafka.producer;

import io.dropwizard.lifecycle.Managed;

public class ManagedProducer implements Managed {

    private final KafkaProducer<?, ?> producer;

    public ManagedProducer(final KafkaProducer<?, ?> producer) {
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
