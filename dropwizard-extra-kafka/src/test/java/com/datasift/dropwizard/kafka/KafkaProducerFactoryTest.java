package com.datasift.dropwizard.kafka;

import com.datasift.dropwizard.kafka.KafkaProducerFactory;
import com.google.common.io.Resources;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;

import kafka.producer.ProducerConfig;
import kafka.serializer.DefaultEncoder;
import org.junit.Before;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.io.File;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static com.datasift.dropwizard.kafka.KafkaProducerFactory.DEFAULT_BROKER_PORT;

/**
 * TODO: Document
 */
public class KafkaProducerFactoryTest {

    private KafkaProducerFactory factory;
    private ProducerConfig config;

    @Before
    public void setup() throws Exception {
        final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        factory = new ConfigurationFactory<>(KafkaProducerFactory.class, validator, Jackson.newObjectMapper(), "dw")
                .build(new File(Resources.getResource("yaml/producer.yaml").toURI()));
        config = KafkaProducerFactory.toProducerConfig(factory, DefaultEncoder.class, null, null, "test");
    }

    @Test
    public void testExplicitBrokers() {
        assertThat("explcitly defined brokers are correctly parsed",
                config.brokerList(),
                equalTo("localhost:4321,192.168.10.12:123,localhost:"
                        + DEFAULT_BROKER_PORT + ",192.168.4.21:" + DEFAULT_BROKER_PORT));
    }
}
