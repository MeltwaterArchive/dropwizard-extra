package com.datasift.dropwizard.kafka;

import com.google.common.io.Resources;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import kafka.producer.ProducerConfig;
import org.junit.Before;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.io.File;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Tests {@link KafkaProducerFactory}.
 */
public class KafkaProducerFactoryTest {

    private KafkaProducerFactory factory = null;

    @Before
    public void setup() throws Exception {
        final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        factory = new ConfigurationFactory<>(KafkaProducerFactory.class, validator, Jackson.newObjectMapper(), "dw")
                .build(new File(Resources.getResource("yaml/producer.yaml").toURI()));
    }

    // Helper to construct Scala Lists
    private <T> scala.collection.immutable.List<T> scalaList(T... javaArray) {
        return scala.collection.JavaConversions.asScalaIterable(Arrays.asList(javaArray)).toList();
    }

    @Test
    public void asProducerConfigTest() {
        final ProducerConfig producerConfig = factory.asProducerConfig();
        assertEquals("myhost1:myport1, myhost2:myport2", producerConfig.brokerList());
        assertEquals(1, producerConfig.requestRequiredAcks());
        assertEquals(400, producerConfig.requestTimeoutMs());
        assertEquals("async", producerConfig.producerType());
        assertEquals("some.serializer.Class", producerConfig.serializerClass());
        assertEquals("some.serializer.key.Class", producerConfig.keySerializerClass());
        assertEquals("some.partitioner.Class", producerConfig.partitionerClass());
        assertEquals("gzip", producerConfig.compressionCodec().name());
        assertEquals(scalaList("topic", "othertopic"), producerConfig.compressedTopics());
        assertEquals(3, producerConfig.messageSendMaxRetries());
        assertEquals(20, producerConfig.retryBackoffMs());
        assertEquals(3000, producerConfig.topicMetadataRefreshIntervalMs());
        assertEquals(100, producerConfig.queueBufferingMaxMs());
        assertEquals(50, producerConfig.queueBufferingMaxMessages());
        assertEquals(-1, producerConfig.queueEnqueueTimeoutMs());
        assertEquals(50, producerConfig.batchNumMessages());
        assertEquals(5*1024, producerConfig.sendBufferBytes());
        assertEquals("test.client", producerConfig.clientId());
    }
}
