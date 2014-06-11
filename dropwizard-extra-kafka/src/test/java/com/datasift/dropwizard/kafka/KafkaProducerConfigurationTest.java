package com.datasift.dropwizard.kafka;

import io.dropwizard.jackson.Jackson;
import com.datasift.dropwizard.zookeeper.ZooKeeperFactory;
import com.google.common.io.Resources;
import io.dropwizard.configuration.ConfigurationFactory;
import org.junit.Before;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.io.File;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/**
 * Tests {@link KafkaConsumerFactory}.
 */
public class KafkaProducerConfigurationTest {

    private KafkaProducerFactory factory = null;

    @Before
    public void setup() throws Exception {
        final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        factory = new ConfigurationFactory<>(KafkaProducerFactory.class, validator, Jackson.newObjectMapper(), "dw")
                .build(new File(Resources.getResource("yaml/producer.yaml").toURI()));
    }

    @Test
    public void testMetaDataBrokerList() {
        assertThat("metadata.broker.list is correctly configured", factory.getMetadataBrokerList(), is("myhost1:myport1, myhost2:myport2"));
    }

    @Test
    public void testRequestRequiredAcks(){
        assertThat("request.required.acks is correctly configured", Integer.toString(factory.getRequestRequiredAcks()), is("1"));
    }

    @Test
    public void testRequestTimeoutMs(){
        assertThat("request.timeout.ms is correctly configured", Integer.toString(factory.getRequestTimeout()), is("40000"));
    }

    @Test
    public void testProducerType(){
        assertThat("producer.type is correctly configured", factory.getProducerType(), is("async"));
    }

    @Test
    public void testSerializerClass(){
        assertThat("serializer.class is correctly configured", factory.getSerializerClass(), is("kafka.serializer.StringEncoder"));
    }

    @Test
    public void testKeySerializerClass(){
        assertThat("key.serializer.class is correctly configured", factory.getKeySerializerClass(), is("kafka.serializer.StringEncoder"));
    }

    @Test
    public void testPartitionerClass(){
        assertThat("partitioner.class is correctly configured", factory.getPartitionerClass(), is("com.datasift.dropwizard.kafka.SimplePartitioner"));
    }

    @Test
    public void testCompressionCodec(){
        assertThat("compression.codec is correctly configured", factory.getCompressionCodec(), is("gzip"));
    }

    @Test
    public void testCompressedTopics(){
        assertThat("compressed.topics is correctly configured", factory.getCompressedTopics(), is("topic1, topic2"));
    }

    @Test
    public void testMessageSendMaxRetries(){
        assertThat("message.send.max.retries is correctly configured", Integer.toString(factory.getMessageSendMaxRetries()), is("5"));
    }

    @Test
    public void testRetryBackoffMS(){
        assertThat("retry.backoff.ms is correctly configured", Integer.toString(factory.getRetryBackoffMilliSecs()), is("200"));
    }

    @Test
    public void testTopicMetadataRefreshIntervalMs(){
        assertThat("topic.metadata.refresh.interval.ms is correctly configured", Integer.toString(factory.getTopicMetadataRefreshIntervalMilliSecs()), is("300000"));
    }

    @Test
    public void testQueueBufferingMaxMs(){
        assertThat("queue.buffering.max.ms is correctly configured", Integer.toString(factory.getQueueBufferingMaxMilliSecs()), is("1000"));
    }

    @Test
    public void testQueueBufferingMaxMessages(){
        assertThat("queue.buffering.max.messages is correctly configured", Integer.toString(factory.getQueueBufferingMaxMessages()), is("500"));
    }

    @Test
    public void testQueueEnqueueTimeoutMs(){
        assertThat("queue.enqueue.timeout.ms is correctly configured", Integer.toString(factory.getQueueEnqueueTimeoutMilliSecs()), is("0"));
    }

    @Test
    public void testBatchNumMessages(){
        assertThat("batch.num.messages is correctly configured", Integer.toString(factory.getBatchNumMessages()), is("500"));
    }

    @Test
    public void testSendBufferBytes(){
        assertThat("send.buffer.bytes is correctly configured", Integer.toString(factory.getSendBufferBytes()), is("10240"));
    }

    @Test
    public void testClientId(){
        assertThat("test.client.id is correctly configured", factory.getClientId(), is("test.client"));
    }
}
