package com.datasift.dropwizard.kafka.producer;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import kafka.javaapi.producer.ProducerData;

import java.util.List;

public class InstrumentedProducer<K, V> implements KafkaProducer<K, V> {

    private final KafkaProducer<K, V> underlying;
    private final Meter sentMessages;

    public InstrumentedProducer(final KafkaProducer<K, V> underlying,
                                final MetricRegistry registry,
                                final String name) {
        this.underlying = underlying;
        this.sentMessages = registry.meter(MetricRegistry.name(name, "sent"));
    }

    @Override
    public void send(final String topic, final V message) {
        underlying.send(topic, message);
        sentMessages.mark();
    }

    @Override
    public void send(final String topic, final K key, final V message) {
        underlying.send(topic, key, message);
        sentMessages.mark();
    }

    @Override
    public void send(final ProducerData<K, V> data) {
        underlying.send(data);
        sentMessages.mark();
    }

    @Override
    public void send(final List<ProducerData<K, V>> data) {
        underlying.send(data);
        sentMessages.mark(data.size());
    }

    @Override
    public void close() {
        underlying.close();
    }
}
