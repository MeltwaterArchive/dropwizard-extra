package com.datasift.dropwizard.kafka.producer;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import java.util.List;

/**
 * A {@link Producer} that is instrumented with metrics.
 */
public class InstrumentedProducer<K, V> implements KafkaProducer<K, V> {

    private final KafkaProducer<K, V> underlying;
    private final Meter sentMessages;

    public InstrumentedProducer(final KafkaProducer<K, V> underlying,
                                final MetricRegistry registry,
                                final String name) {
        this.underlying = underlying;
        this.sentMessages = registry.meter(MetricRegistry.name(name, "sent"));
    }

    public void send(final String topic, final V message) {
        underlying.send(topic, message);
        sentMessages.mark();
    }

    public void send(final String topic, final K key, final V message) {
        underlying.send(topic, key, message);
        sentMessages.mark();
    }

    public void send(final KeyedMessage<K, V> message) {
        underlying.send(message);
        sentMessages.mark();
    }

    public void send(final List<KeyedMessage<K, V>> messages) {
        underlying.send(messages);
        sentMessages.mark(messages.size());
    }

    public void close() {
        underlying.close();
    }
}
