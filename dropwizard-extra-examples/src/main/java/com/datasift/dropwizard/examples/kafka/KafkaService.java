package com.datasift.dropwizard.examples.kafka;
/**
 * Created by ram on 6/2/14.
 */
import com.datasift.dropwizard.kafka.KafkaConsumerFactory;
import com.datasift.dropwizard.kafka.KafkaProducerConfiguration;
import com.datasift.dropwizard.kafka.consumer.KafkaConsumer;
import io.dropwizard.Application;
import io.dropwizard.configuration.ConfigurationException;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.setup.Environment;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.lifecycle.Managed;
import kafka.javaapi.producer.Producer;

import javax.validation.Validation;
import javax.validation.Validator;
import java.io.File;
import java.io.IOException;

public class KafkaService extends Application<KafkaEnricherConfiguration> implements Managed {
    private static final String DROPWIZARD_PREFIX = "dw";
    private static final String USER_DIR = "user.dir";
    private Producer producer;
    private KafkaConsumer consumer;
    public static void main(String[] args) throws Exception {
        KafkaService ks = new KafkaService();
        ks.run(args);
    }

    @Override
    public void initialize(Bootstrap<KafkaEnricherConfiguration> bootstrap) {
        //bootstrap.addBundle();
        //bootstrap.addCommand();
    }

    private KafkaConsumer buildConsumer(String configFile,
                                        String name,
                                        Environment environment,
                                        Producer producer,
                                        KafkaEnricherConfiguration configuration) throws Exception{
        // Build the consumer
        final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        KafkaConsumerFactory kcf = new ConfigurationFactory<>(KafkaConsumerFactory.class, validator,
                Jackson.newObjectMapper(),
                DROPWIZARD_PREFIX)
                .build(new File(System.getProperty(USER_DIR)
                        + File.separator
                        + configFile));
        MessageEnricher enricher = new MessageEnricher(configuration);
        KafkaStreamProcessor ksp = new KafkaStreamProcessor(producer, configuration, enricher);
        KafkaConsumerFactory.KafkaConsumerBuilder kcb = kcf.processWith(ksp);
        return kcb.build(environment, name);
    }
    private Producer buildProducer(String configName) throws IOException, ConfigurationException {

        final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        KafkaProducerConfiguration kpc = new ConfigurationFactory<>(KafkaProducerConfiguration.class, validator,
                Jackson.newObjectMapper(),
                DROPWIZARD_PREFIX)
                .build(new File(System.getProperty(USER_DIR)
                        + File.separator
                        + configName));

       return new Producer<String, String>(kpc.asProducerConfig());

    }
    @Override
    public void run(KafkaEnricherConfiguration configuration, Environment environment) throws Exception {

        environment.jersey().register(new KafkaEnricherResource());

        //TODO: Get the name (kafka-consumer.yml) from the config file
        this.producer   = buildProducer("kafka-producer.yml");


        //TODO: Get the name from the config file
        //TODO: Get the name (kafka-consumer.yml) from the config file
        this.consumer = buildConsumer("kafka-consumer.yml", "test-me-not", environment, producer, configuration);

    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {
        this.producer.close();
        this.consumer.stop();
    }
}