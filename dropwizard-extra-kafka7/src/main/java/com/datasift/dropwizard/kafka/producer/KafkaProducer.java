package com.datasift.dropwizard.kafka.producer;

import kafka.javaapi.producer.ProducerData;

import java.util.List;

public interface KafkaProducer<K, V> {

    void send(ProducerData<K, V> data);
    void send(List<ProducerData<K, V>> data);
    void close();
}
