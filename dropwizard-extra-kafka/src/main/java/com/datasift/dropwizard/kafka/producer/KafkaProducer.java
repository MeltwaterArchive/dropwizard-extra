package com.datasift.dropwizard.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import java.util.List;

/**
 * Interface for {@link Producer} proxies.
 */
public interface KafkaProducer<K, V> {

    public void send(KeyedMessage<K, V> message);

    public void send(List<KeyedMessage<K, V>> messages);

    public void close();
}
