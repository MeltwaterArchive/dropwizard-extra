package com.datasift.dropwizard.kafka.producer;

import io.dropwizard.lifecycle.Managed;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import java.util.List;

/**
 * Wraps a kafka Producer with an implementation of dropwizard's Managed interface,
 * so you can have your application lifecycle manage this (i.e. call close to shut it down).
 * Proxies send methods through to the underlying Producer.
 */
public class KafkaProducer<K,V> implements Managed {
    private final Producer<K,V> producer;

    public KafkaProducer(final Producer<K,V> producer) {
        this.producer = producer;
    }

    public void send(KeyedMessage<K,V> message) {
        producer.send(message);
    }

    public void send(List<KeyedMessage<K,V>> messages) {
        producer.send(messages);
    }

    @Override
    public void start() throws Exception {}

    @Override
    public void stop() throws Exception {
        producer.close();
    }
}
