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
public class KafkaConsumerConfigurationTest {

    private KafkaConsumerFactory factory = null;

    @Before
    public void setup() throws Exception {
        final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        factory = new ConfigurationFactory<>(KafkaConsumerFactory.class, validator, Jackson.newObjectMapper(), "dw")
                .build(new File(Resources.getResource("yaml/consumer.yaml").toURI()));
    }

    @Test
    public void testZooKeeper() {
        assertThat("has ZooKeeperConfiguration",
                factory.getZookeeper(),
                instanceOf(ZooKeeperFactory.class));
    }

    @Test
    public void testGroup() {
        assertThat("group is correctly configured", factory.getGroup(), is("test"));
    }

    @Test
    public void testPartitions() {
        assertThat("has correct partition configuration",
                   factory.getPartitions(),
                   allOf(hasEntry("foo", 1), hasEntry("bar", 2)));
    }

    @Test
    public void testRebalanceRetries() {
        assertThat("rebalanceRetries is overridden to 5",
                   factory.getRebalanceRetries(),
                   is(5));
    }
}
