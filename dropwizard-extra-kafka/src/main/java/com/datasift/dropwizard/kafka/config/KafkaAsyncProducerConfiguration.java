package com.datasift.dropwizard.kafka.config;

import com.yammer.dropwizard.util.Duration;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * TODO: Document
 */
public class KafkaAsyncProducerConfiguration {

    /** maximum time for buffering data in the producer queue */
    @JsonProperty
    @NotNull
    protected Duration queueTime = Duration.seconds(5);

    /** maximum size of the queue for buffering data */
    @JsonProperty
    @Min(1)
    protected int queueSize = 10000;

    /** number of messages to batch together before being dispatched */
    @JsonProperty
    @Min(1)
    protected int batchSize = 200;

    public Duration getQueueTime() {
        return queueTime;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public int getBatchSize() {
        return batchSize;
    }
}
