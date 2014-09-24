package com.datasift.dropwizard.kafka;

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

import static com.datasift.dropwizard.kafka.KafkaProducerFactory.DEFAULT_BROKER_PORT;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

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
        config = KafkaProducerFactory.toProducerConfig(factory, new DefaultEncoder(), null);
    }

    @Test
    public void testExplicitBrokers() {
        assertThat("explcitly defined brokers are correctly parsed",
                config.brokerList(),
                equalTo("0:localhost:4321,1:192.168.10.12:123,2:localhost:"
                        + DEFAULT_BROKER_PORT + ",3:192.168.4.21:" + DEFAULT_BROKER_PORT));
    }
}
