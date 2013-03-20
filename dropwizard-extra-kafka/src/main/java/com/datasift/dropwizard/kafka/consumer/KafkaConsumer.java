package com.datasift.dropwizard.kafka.consumer;

/**
 * Interface for consuming a stream of messages from Kafka.
 */
public interface KafkaConsumer {

    /**
     * Commit the offsets of the current position in the message streams.
     *
     * @see kafka.consumer.ConsumerConnector#commitOffsets()
     */
    public void commitOffsets();

    /**
     * Determines if this {@link KafkaConsumer} is currently consuming.
     *
     * @return true if this {@link KafkaConsumer} is currently consuming from at least one
     *              partition; otherwise, false.
     */
    public boolean isRunning();
}
