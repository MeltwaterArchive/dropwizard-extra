package com.datasift.dropwizard.kafka.producer;

import kafka.javaapi.producer.ProducerData;

import java.util.List;

public interface KafkaProducer<K, V> {

    public void send(ProducerData<K, V> data);
    public void send(List<ProducerData<K, V>> data);
    public void close();
}
