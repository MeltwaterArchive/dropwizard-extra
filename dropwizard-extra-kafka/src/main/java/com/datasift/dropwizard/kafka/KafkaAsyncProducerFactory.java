package com.datasift.dropwizard.kafka;

import com.codahale.dropwizard.util.Duration;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * Configuration for the asynchronous Kafka producer.
 */
public class KafkaAsyncProducerFactory extends KafkaProducerFactory {

    /**
     * Maximum time for buffering data in the producer queue.
     */
    @JsonProperty
    @NotNull
    protected Duration queueTime = Duration.seconds(5);

    /**
     * Maximum number of messages in the send queue before triggering a flush.
     */
    @JsonProperty
    @Min(1)
    protected int queueSize = 10000;

    /**
     * Number of messages to batch together before being dispatched.
     */
    @JsonProperty
    @Min(1)
    protected int batchSize = 200;

    /**
     * @see KafkaAsyncProducerFactory#queueTime
     */
    public Duration getQueueTime() {
        return queueTime;
    }

    /**
     * @see KafkaAsyncProducerFactory#queueSize
     */
    public int getQueueSize() {
        return queueSize;
    }

    /**
     * @see KafkaAsyncProducerFactory#batchSize
     */
    public int getBatchSize() {
        return batchSize;
    }
}
