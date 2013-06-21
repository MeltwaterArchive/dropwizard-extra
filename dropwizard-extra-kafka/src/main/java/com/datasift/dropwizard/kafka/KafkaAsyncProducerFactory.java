package com.datasift.dropwizard.kafka;

import com.codahale.dropwizard.util.Duration;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * Configuration for the asynchronous Kafka producer.
 */
public class KafkaAsyncProducerFactory {

    @NotNull
    protected Duration queueTime = Duration.seconds(5);

    @Min(1)
    protected int queueSize = 10000;

    @Min(1)
    protected int batchSize = 200;

    /**
     * Returns the maximum time for buffering data in the producer queue.
     *
     * @return the maximum time for buffering data in the producer queue.
     */
    @JsonProperty
    public Duration getQueueTime() {
        return queueTime;
    }

    /**
     * Sets the maximum time for buffering data in the producer queue.
     *
     * @param time the maximum time for buffering data in the producer queue.
     */
    @JsonProperty
    public void setQueueTime(final Duration time) {
        this.queueTime = time;
    }

    /**
     * Returns the number of messages in the send queue before triggering a flush.
     *
     * @return the maximum number of messages in the send queue before triggering a flush.
     */
    @JsonProperty
    public int getQueueSize() {
        return queueSize;
    }

    /**
     * Sets the number of messages in the send queue before triggering a flush.
     *
     * @param size the maximum number of messages in the send queue before triggering a flush.
     */
    @JsonProperty
    public void setQueueSize(final int size) {
        this.queueSize = size;
    }

    /**
     * Returns the number of messages to batch together before being dispatched.
     *
     * @return the number of messages to batch together before being dispatched.
     */
    @JsonProperty
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the number of messages to batch together before being dispatched.
     *
     * @param size the number of messages to batch together before being dispatched.
     */
    @JsonProperty
    public void getBatchSize(final int size) {
        this.batchSize = size;
    }
}
