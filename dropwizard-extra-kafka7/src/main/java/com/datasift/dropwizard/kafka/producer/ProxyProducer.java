package com.datasift.dropwizard.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;

import java.util.List;

public class ProxyProducer<K, V> implements KafkaProducer<K, V> {

    private final Producer<K, V> producer;

    public ProxyProducer(final Producer<K, V> producer) {
        this.producer = producer;
    }

    @Override
    public void send(final ProducerData<K, V> data) {
        producer.send(data);
    }

    @Override
    public void send(final List<ProducerData<K, V>> data) {
        producer.send(data);
    }

    @Override
    public void close() {
        producer.close();
    }
}
