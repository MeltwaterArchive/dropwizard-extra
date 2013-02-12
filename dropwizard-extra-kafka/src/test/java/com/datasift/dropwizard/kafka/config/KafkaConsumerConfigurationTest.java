package com.datasift.dropwizard.kafka.config;

import com.datasift.dropwizard.zookeeper.config.ZooKeeperConfiguration;
import com.google.common.io.Resources;
import com.yammer.dropwizard.config.ConfigurationFactory;
import com.yammer.dropwizard.validation.Validator;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/** Tests {@link KafkaConsumerConfiguration} */
public class KafkaConsumerConfigurationTest {

    private KafkaConsumerConfiguration config = null;

    @Before
    public void setup() throws Exception {
        config = ConfigurationFactory
                .forClass(KafkaConsumerConfiguration.class, new Validator())
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
