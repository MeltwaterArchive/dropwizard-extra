package com.datasift.dropwizard.kafka.producer;

import io.dropwizard.lifecycle.Managed;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import java.io.Closeable;
import java.util.List;

/**
 * Interface for {@link Producer} proxies.
 */
public interface KafkaProducer<K, V> extends Managed, Closeable {

    public void send(KeyedMessage<K, V> message);

    public void send(List<KeyedMessage<K, V>> messages);

}
