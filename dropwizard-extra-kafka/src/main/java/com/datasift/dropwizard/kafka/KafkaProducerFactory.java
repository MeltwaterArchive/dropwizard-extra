package com.datasift.dropwizard.kafka;

import com.datasift.dropwizard.kafka.producer.InstrumentedProducer;
import com.datasift.dropwizard.kafka.producer.KafkaProducer;
import com.datasift.dropwizard.kafka.producer.ManagedProducer;
import com.datasift.dropwizard.kafka.util.Compression;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;
import io.dropwizard.util.Size;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.validation.MinDuration;
import kafka.javaapi.producer.Producer;
import kafka.producer.Partitioner;
import kafka.producer.ProducerConfig;
import kafka.serializer.Encoder;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.net.InetSocketAddress;
import java.util.Properties;

/**
 * Configuration for the Kafka producer.
 * <p/>
 * By default, the producer will be synchronous, blocking the calling thread until the message has
 * been sent.
 * <p/>
 * To use an asynchronous producer, set {@link KafkaProducerFactory#async} with the desired
 * properties.
 */
public class KafkaProducerFactory extends KafkaClientFactory {

    static final int DEFAULT_BROKER_PORT = 6667;

    /**
     * The acknowledgements to wait for before considering a message as sent.
     */
    public enum Acknowledgement {
        NEVER(0), LEADER(1), ALL(-1);

        private final int value;

        private Acknowledgement(final int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    @NotEmpty
    protected ImmutableSet<InetSocketAddress> brokers = ImmutableSet.of();

    @NotNull
    protected Acknowledgement acknowledgement = Acknowledgement.ALL;

    @NotNull
    @MinDuration(0)
    protected Duration requestTimeout = Duration.seconds(1);

    protected boolean async = false;

    @NotNull
    protected Compression compression = Compression.parse("none");

    @NotNull
    protected ImmutableSet<String> compressedTopics = ImmutableSet.of();

    @Min(0)
    protected int maxRetries = 3;

    @NotNull
    @MinDuration(0)
    protected Duration retryBackOff = Duration.milliseconds(100);

    @NotNull
    @MinDuration(0)
    protected Duration metadataRefreshInterval = Duration.minutes(10);

    @NotNull
    @MinDuration(0)
    protected Duration asyncBatchInterval = Duration.milliseconds(500);

    @Min(1)
    protected int asyncBatchSize = 200;

    @Min(1)
    protected int asyncBufferSize = 10000;

    @NotNull
    protected Optional<Duration> asyncBlockTimeout = Optional.absent();

    protected Size sendBufferSize = Size.kilobytes(100);

    @NotNull
    protected Optional<String> clientIdSuffix = Optional.absent();

    @JsonProperty("brokers")
    public ImmutableSet<InetSocketAddress> getBrokers() {
        return brokers;
    }

    @JsonProperty("brokers")
    public void setBrokers(final ImmutableSet<InetSocketAddress> brokers) {
        this.brokers = brokers;
    }

    @JsonProperty("acknowledgement")
    public Acknowledgement getAcknowledgement() {
        return acknowledgement;
    }

    @JsonProperty("acknowledgement")
    public void setAcknowledgement(final Acknowledgement acknowledgement) {
        this.acknowledgement = acknowledgement;
    }

    @JsonProperty("requestTimeout")
    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    @JsonProperty("requestTimeout")
    public void setRequestTimeout(final Duration requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    @JsonProperty("async")
    public boolean isAsync() {
        return async;
    }

    @JsonProperty("async")
    public void setAsync(final boolean async) {
        this.async = async;
    }

    @JsonProperty("compression")
    public Compression getCompression() {
        return compression;
    }

    @JsonProperty("compression")
    public void setCompression(final Compression compression) {
        this.compression = compression;
    }

    @JsonProperty("compressedTopics")
    public ImmutableSet<String> getCompressedTopics() {
        return compressedTopics;
    }

    @JsonProperty("compressedTopics")
    public void setCompressedTopics(final ImmutableSet<String> compressedTopics) {
        this.compressedTopics = compressedTopics;
    }

    @JsonProperty("maxRetries")
    public int getMaxRetries() {
        return maxRetries;
    }

    @JsonProperty("maxRetries")
    public void setMaxRetries(final int maxRetries) {
        this.maxRetries = maxRetries;
    }

    @JsonProperty("retryBackOff")
    public Duration getRetryBackOff() {
        return retryBackOff;
    }

    @JsonProperty("retryBackOff")
    public void setRetryBackOff(final Duration retryBackOff) {
        this.retryBackOff = retryBackOff;
    }

    @JsonProperty("metadataRefreshInterval")
    public Duration getMetadataRefreshInterval() {
        return metadataRefreshInterval;
    }

    @JsonProperty("metadataRefreshInterval")
    public void setMetadataRefreshInterval(final Duration metadataRefreshInterval) {
        this.metadataRefreshInterval = metadataRefreshInterval;
    }

    @JsonProperty("asyncBatchInterval")
    public Duration getAsyncBatchInterval() {
        return asyncBatchInterval;
    }

    @JsonProperty("asyncBatchInterval")
    public void setAsyncBatchInterval(final Duration asyncBatchInterval) {
        this.asyncBatchInterval = asyncBatchInterval;
    }

    @JsonProperty("asyncBatchSize")
    public int getAsyncBatchSize() {
        return asyncBatchSize;
    }

    @JsonProperty("asyncBatchSize")
    public void setAsyncBatchSize(final int asyncBatchSize) {
        this.asyncBatchSize = asyncBatchSize;
    }

    @JsonProperty("asyncBufferSize")
    public int getAsyncBufferSize() {
        return asyncBufferSize;
    }

    @JsonProperty("asyncBufferSize")
    public void setAsyncBufferSize(final int asyncBufferSize) {
        this.asyncBufferSize = asyncBufferSize;
    }

    @JsonProperty("asyncBlockTimeout")
    public Optional<Duration> getAsyncBlockTimeout() {
        return asyncBlockTimeout;
    }

    @JsonProperty("asyncBlockTimeout")
    public void setAsyncBlockTimeout(final Optional<Duration> asyncBlockTimeout) {
        this.asyncBlockTimeout = asyncBlockTimeout;
    }

    @JsonProperty("sendBufferSize")
    public Size getSendBufferSize() {
        return sendBufferSize;
    }

    @JsonProperty("sendBufferSize")
    public void setSendBufferSize(final Size sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
    }

    @JsonProperty("clientIdSuffix")
    public Optional<String> getClientIdSuffix() {
        return clientIdSuffix;
    }

    @JsonProperty("clientIdSuffix")
    public void setClientIdSuffix(final Optional<String> clientIdSuffix) {
        this.clientIdSuffix = clientIdSuffix;
    }

    public <V> KafkaProducer<?, V> build(final Class<? extends Encoder<V>> messageEncoder,
                                    final Environment environment,
                                    final String name) {
        return build(messageEncoder, null, environment, name);
    }

    public <K, V> KafkaProducer<K, V> build(final Class<? extends Encoder<K>> keyEncoder,
                                       final Class<? extends Encoder<V>> messageEncoder,
                                       final Environment environment,
                                       final String name) {
        return build(keyEncoder, messageEncoder, null, environment, name);
    }

    public <K, V> KafkaProducer<K, V> build(final Class<? extends Encoder<K>> keyEncoder,
                                       final Class<? extends Encoder<V>> messageEncoder,
                                       final Class<? extends Partitioner> partitioner,
                                       final Environment environment,
                                       final String name) {
        final Producer<K, V> producer = build(keyEncoder, messageEncoder, partitioner, name);
        environment.lifecycle().manage(new ManagedProducer(producer));
        return new InstrumentedProducer<>(
                producer,
                environment.metrics(),
                name);
    }

    public <K, V> Producer<K, V> build(final Class<? extends Encoder<K>> keyEncoder,
                                       final Class<? extends Encoder<V>> messageEncoder,
                                       final Class<? extends Partitioner> partitioner,
                                       final String name) {
        return new Producer<>(
                toProducerConfig(this, messageEncoder, keyEncoder, partitioner, name));
    }

    static <K, V> ProducerConfig toProducerConfig(final KafkaProducerFactory factory,
                                                  final Class<? extends Encoder<V>> messageEncoder,
                                                  final Class<? extends Encoder<K>> keyEncoder,
                                                  final Class<? extends Partitioner> partitioner,
                                                  final String name) {
        final Properties properties = new Properties();

        final StringBuilder sb = new StringBuilder(10*factory.getBrokers().size());
        for (final InetSocketAddress addr : factory.getBrokers()) {
            final int port = addr.getPort() == 0 ? DEFAULT_BROKER_PORT : addr.getPort();
            sb.append(addr.getHostString()).append(':').append(port).append(',');
        }
        properties.setProperty(
                "metadata.broker.list", sb.substring(0, sb.length() - 1));
        properties.setProperty(
                "request.required.acks", Integer.toString(factory.getAcknowledgement().getValue()));
        properties.setProperty(
                "request.timeout.ms", Long.toString(factory.getRequestTimeout().toMilliseconds()));
        properties.setProperty("producer.type", factory.isAsync() ? "async" : "sync");
        properties.setProperty("serializer.class", messageEncoder.getCanonicalName());

        if (keyEncoder != null) {
            properties.setProperty("key.serializer.class", keyEncoder.getCanonicalName());
        }

        if (partitioner != null) {
            properties.setProperty("partitioner.class", partitioner.getCanonicalName());
        }

        properties.setProperty("compression.codec", factory.getCompression().getCodec().name());

        if (!factory.getCompressedTopics().isEmpty()) {
            properties.setProperty(
                    "compressed.topics", Joiner.on(',').join(factory.getCompressedTopics()));
        }

        properties.setProperty(
                "message.send.max.retries", Integer.toString(factory.getMaxRetries()));
        properties.setProperty(
                "retry.backoff.ms", Long.toString(factory.getRetryBackOff().toMilliseconds()));
        properties.setProperty(
                "topic.metadata.refresh.interval.ms",
                Long.toString(factory.getMetadataRefreshInterval().toMilliseconds()));
        properties.setProperty(
                "queue.buffering.max.ms",
                Long.toString(factory.getAsyncBatchInterval().toMilliseconds()));
        properties.setProperty(
                "queue.buffering.max.messages", Integer.toString(factory.getAsyncBufferSize()));
        properties.setProperty(
                "queue.enqueue.timeout.ms",
                Long.toString(factory.getAsyncBlockTimeout()
                        .or(Duration.milliseconds(-1)).toMilliseconds()));
        properties.setProperty("batch.num.messages", Integer.toString(factory.getAsyncBatchSize()));
        properties.setProperty(
                "send.buffer.bytes", Long.toString(factory.getSendBufferSize().toBytes()));

        final StringBuilder clientId = new StringBuilder(name);
        if (factory.getClientIdSuffix().isPresent()) {
            clientId.append('-').append(factory.getClientIdSuffix().get());
        }
        properties.setProperty("client.id", clientId.toString());

        return new ProducerConfig(properties);
    }
}
