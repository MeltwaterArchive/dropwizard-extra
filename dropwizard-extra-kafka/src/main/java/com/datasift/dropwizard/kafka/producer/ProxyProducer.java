package com.datasift.dropwizard.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import java.util.List;

public class ProxyProducer<K, V> implements KafkaProducer<K, V> {

    private final Producer<K, V> producer;

    public ProxyProducer(final Producer<K, V> producer) {
        this.producer = producer;
    }

    @Override
    public void send(final String topic, final V message) {
        producer.send(new KeyedMessage<K, V>(topic, message));
    }

    @Override
    public void send(final String topic, final K key, final V message) {
        producer.send(new KeyedMessage<>(topic, key, message));
    }

    @Override
    public void send(final KeyedMessage<K, V> data) {
        producer.send(data);
    }

    @Override
    public void send(final List<KeyedMessage<K, V>> data) {
        producer.send(data);
    }

    @Override
    public void close() {
        producer.close();
    }
}
