package com.datasift.dropwizard.kafka;

import com.datasift.dropwizard.kafka.producer.InstrumentedProducer;
import com.datasift.dropwizard.kafka.producer.KafkaProducer;
import com.datasift.dropwizard.kafka.producer.ManagedProducer;
import com.datasift.dropwizard.kafka.producer.ProxyProducer;
import com.datasift.dropwizard.kafka.util.Compression;
import com.datasift.dropwizard.zookeeper.ZooKeeperFactory;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;
import io.dropwizard.util.Size;
import io.dropwizard.validation.ValidationMethod;
import kafka.javaapi.producer.Producer;
import kafka.producer.Partitioner;
import kafka.producer.ProducerConfig;
import kafka.serializer.Encoder;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Properties;

public class KafkaProducerFactory extends KafkaClientFactory {

    static final int DEFAULT_BROKER_PORT = 9092;

    @Valid
    private Optional<ZooKeeperFactory> zookeeper = Optional.absent();

    @Valid
    private ImmutableMap<Integer, InetSocketAddress> brokers = ImmutableMap.of();

    private boolean async = false;

    private Size sendBufferSize = Size.kilobytes(100);

    private Duration connectionTimeout = Duration.seconds(5);

    @Min(1)
    private long reconnectInterval = 30000;

    private Optional<Duration> reconnectTimeInterval = Optional.of(Duration.seconds(10000));

    private Size maxMessageSize = Size.megabytes(1);

    private Compression compression = Compression.parse("none");

    private ImmutableSet<String> compressedTopics = ImmutableSet.of();

    private int zookeeperReadRetries = 3;

    private Duration queueTime = Duration.seconds(5);

    private long queueSize = 10000;

    private long batchSize = 200;

    @JsonIgnore
    @ValidationMethod(message = "only one of 'zookeeper' and 'brokers' may be set")
    public boolean isOneDiscoveryType() {
        return zookeeper.isPresent() ^ (!brokers.isEmpty());
    }

    @JsonIgnore
    @ValidationMethod(message = "one of 'zookeeper' and 'brokers' must be set")
    public boolean isZookeeperOrBrokers() {
        return zookeeper.isPresent() || (!brokers.isEmpty());
    }

    @JsonProperty
    public Optional<ZooKeeperFactory> getZookeeper() {
        return zookeeper;
    }

    @JsonProperty
    public void setZookeeper(final Optional<ZooKeeperFactory> zookeeper) {
        this.zookeeper = zookeeper;
    }

    @JsonProperty
    public ImmutableMap<Integer, InetSocketAddress> getBrokers() {
        return brokers;
    }

    @JsonProperty
    public void setBrokers(final ImmutableMap<Integer, InetSocketAddress> brokers) {
        this.brokers = brokers;
    }

    @JsonProperty
    public boolean isAsync() {
        return async;
    }

    @JsonProperty
    public void setAsync(final boolean async) {
        this.async = async;
    }

    @JsonProperty
    public Size getSendBufferSize() {
        return sendBufferSize;
    }

    @JsonProperty
    public void setSendBufferSize(final Size sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
    }

    @JsonProperty
    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    @JsonProperty
    public void setConnectionTimeout(final Duration connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    @JsonProperty
    public long getReconnectInterval() {
        return reconnectInterval;
    }

    @JsonProperty
    public void setReconnectInterval(final long reconnectInterval) {
        this.reconnectInterval = reconnectInterval;
    }

    @JsonProperty
    public Optional<Duration> getReconnectTimeInterval() {
        return reconnectTimeInterval;
    }

    @JsonProperty
    public void setReconnectTimeInterval(final Optional<Duration> reconnectTimeInterval) {
        this.reconnectTimeInterval = reconnectTimeInterval;
    }

    @JsonProperty
    public Size getMaxMessageSize() {
        return maxMessageSize;
    }

    @JsonProperty
    public void setMaxMessageSize(final Size maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    @JsonProperty
    public Compression getCompression() {
        return compression;
    }

    @JsonProperty
    public void setCompression(final Compression compression) {
        this.compression = compression;
    }

    @JsonProperty
    public ImmutableSet<String> getCompressedTopics() {
        return compressedTopics;
    }

    @JsonProperty
    public void setCompressedTopics(final ImmutableSet<String> compressedTopics) {
        this.compressedTopics = compressedTopics;
    }

    @JsonProperty
    public int getZookeeperReadRetries() {
        return zookeeperReadRetries;
    }

    @JsonProperty
    public void setZookeeperReadRetries(final int zookeeperReadRetries) {
        this.zookeeperReadRetries = zookeeperReadRetries;
    }

    @JsonProperty
    public Duration getQueueTime() {
        return queueTime;
    }

    @JsonProperty
    public void setQueueTime(final Duration queueTime) {
        this.queueTime = queueTime;
    }

    @JsonProperty
    public long getQueueSize() {
        return queueSize;
    }

    @JsonProperty
    public void setQueueSize(final long queueSize) {
        this.queueSize = queueSize;
    }

    @JsonProperty
    public long getBatchSize() {
        return batchSize;
    }

    @JsonProperty
    public void setBatchSize(final long batchSize) {
        this.batchSize = batchSize;
    }

    public <K, T> KafkaProducer<K, T> build(final Encoder<T> encoder,
                                            final Environment environment,
                                            final String name) {
        return build(encoder, null, environment, name);
    }

    public <K, T> KafkaProducer<K, T> build(final Encoder<T> encoder,
                                            final Partitioner<K> partitioner,
                                            final Environment environment,
                                            final String name) {
        final KafkaProducer<K, T> producer = build(encoder, partitioner);
        environment.lifecycle().manage(new ManagedProducer(producer));
        return new InstrumentedProducer<>(producer, environment.metrics(), name);
    }

    public <K, T> KafkaProducer<K, T> build(final Encoder<T> encoder) {
        return build(encoder, null);
    }

    public <K, T> KafkaProducer<K, T> build(final Encoder<T> encoder,
                                            final Partitioner<K> partitioner) {
        return new ProxyProducer<>(new Producer<>(
                toProducerConfig(this, encoder, partitioner),
                encoder,
                null,
                null,
                partitioner));
    }

    static ProducerConfig toProducerConfig(final KafkaProducerFactory factory,
                                           final Encoder<?> encoder,
                                           final Partitioner<?> partitioner) {
        final Properties props = new Properties();

        props.setProperty("serializer.class", encoder.getClass().getCanonicalName());

        if (partitioner != null && factory.getBrokers().isEmpty()) {
            props.setProperty("partitioner.class", partitioner.getClass().getCanonicalName());
        }

        props.setProperty("producer.type", factory.isAsync() ? "async" : "sync");

        final Optional<ZooKeeperFactory> zooKeeperFactory = factory.getZookeeper();
        if (zooKeeperFactory.isPresent()) {
            final ZooKeeperFactory zk = zooKeeperFactory.get();
            props.setProperty("zk.connect", zk.getQuorumSpec() + zk.getNamespace());
        } else {
            final StringBuilder sb = new StringBuilder(10*factory.getBrokers().size());
            for (final ImmutableMap.Entry<Integer, InetSocketAddress> e : factory.getBrokers().entrySet()) {
                final String host = e.getValue().getHostString();
                final int port = e.getValue().getPort() == 0 ? DEFAULT_BROKER_PORT : e.getValue().getPort();
                sb.append(e.getKey()).append(':').append(host).append(':').append(port).append(',');
            }
            props.setProperty("broker.list", sb.substring(0, sb.length() - 1));
        }

        props.setProperty("buffer.size", Long.toString(factory.getSendBufferSize().toBytes()));
        props.setProperty("connect.timeout.ms",
                Long.toString(factory.getConnectionTimeout().toMilliseconds()));
        props.setProperty("socket.timeout.ms",
                Long.toString(factory.getSocketTimeout().toMilliseconds()));
        props.setProperty("reconnect.interval", Long.toString(factory.reconnectInterval));
        props.setProperty("reconnect.time.interval.ms",
                Long.toString(factory.reconnectTimeInterval
                        .or(Duration.milliseconds(-1)).toMilliseconds()));
        props.setProperty("max.message.size", Long.toString(factory.maxMessageSize.toBytes()));
        props.setProperty("compression.codec",
                Integer.toString(factory.compression.getCodec().codec()));
        props.setProperty("zk.read.num.retries",
                Integer.toString(factory.getZookeeperReadRetries()));

        final ImmutableSet<String> compressedTopics = factory.getCompressedTopics();
        if (!compressedTopics.isEmpty()) {
            props.setProperty("compressed.topics", Joiner.on(',').join(compressedTopics));
        }

        if (factory.isAsync()) {
            props.setProperty("queue.time", Long.toString(factory.getQueueTime().toMilliseconds()));
            props.setProperty("queue.size", Long.toString(factory.getQueueSize()));
            props.setProperty("batch.size", Long.toString(factory.getBatchSize()));
        }

        return new ProducerConfig(props);
    }
}
