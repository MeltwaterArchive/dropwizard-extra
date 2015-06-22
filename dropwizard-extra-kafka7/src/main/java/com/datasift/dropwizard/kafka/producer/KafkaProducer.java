package com.datasift.dropwizard.kafka.producer;

import kafka.javaapi.producer.ProducerData;

import java.util.List;

public interface KafkaProducer<K, V> {

    void send(String topic, V message);

    void send(String topic, K key, V message);

    void send(ProducerData<K, V> data);

    void send(List<ProducerData<K, V>> data);

    void close();
}
