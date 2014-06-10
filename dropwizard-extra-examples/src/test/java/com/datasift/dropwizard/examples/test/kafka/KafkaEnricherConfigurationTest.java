package com.datasift.dropwizard.examples.test.kafka;


import com.datasift.dropwizard.examples.kafka.KafkaEnricherConfiguration;
import com.datasift.dropwizard.kafka.KafkaConsumerFactory;
import com.datasift.dropwizard.kafka.KafkaProducerFactory;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.io.File;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * Created by ram on 6/5/14.
 */
public class KafkaEnricherConfigurationTest {
    @Test
    public void testPartitionConfiguration() throws Exception{
        final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

        KafkaEnricherConfiguration fc = new ConfigurationFactory<>(KafkaEnricherConfiguration.class, validator,
                Jackson.newObjectMapper(),
                "dw")
                .build(new File(System.getProperty("user.dir")+File.separator+"kafka-service.yml"));
        KafkaConsumerFactory kcf = fc.getKafkaConsumerFactory();
        KafkaProducerFactory kpf = fc.getKafkaProducerFactory();
        String topic = fc.getProducerTopic();
        /*
        assertEquals("Failed to get the expected value for config.file", "kafka-consumer.yml", props.getProperty("config.file"));
        assertEquals("Failed to get the expected value for adsavvy.enriched.map", 2, props.get("adsavvy.track"));
        assertEquals("Failed to get the expected value for client.id", "adsavvy.map.enricher", props.getProperty("client.id"));
        */
    }
}
