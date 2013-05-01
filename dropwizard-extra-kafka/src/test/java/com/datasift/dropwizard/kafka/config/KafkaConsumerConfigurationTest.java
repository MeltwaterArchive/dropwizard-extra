package com.datasift.dropwizard.kafka.config;

import com.datasift.dropwizard.zookeeper.config.ZooKeeperConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import com.codahale.dropwizard.configuration.ConfigurationFactory;
import org.junit.Before;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.io.File;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/** Tests {@link KafkaConsumerConfiguration} */
public class KafkaConsumerConfigurationTest {

    private KafkaConsumerConfiguration config = null;

    @Before
    public void setup() throws Exception {
        final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        config = new ConfigurationFactory<>(KafkaConsumerConfiguration.class, validator, new ObjectMapper(), "dw")
                .build(new File(Resources.getResource("yaml/consumer.yaml").toURI()));
    }

    @Test
    public void testZooKeeper() {
        assertThat("has ZooKeeperConfiguration",
                config.getZookeeper(),
                instanceOf(ZooKeeperConfiguration.class));
    }

    @Test
    public void testGroup() {
        assertThat("group is correctly configured", config.getGroup(), is("test"));
    }

    @Test
    public void testPartitions() {
        assertThat("has correct partition configuration",
                   config.getPartitions(),
                   allOf(hasEntry("foo", 1), hasEntry("bar", 2)));
    }

    @Test
    public void testRebalanceRetries() {
        assertThat("rebalanceRetries is overridden to 5",
                   config.getRebalanceRetries(),
                   is(5));
    }
}
