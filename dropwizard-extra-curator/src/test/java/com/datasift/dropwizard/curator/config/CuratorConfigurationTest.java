package com.datasift.dropwizard.curator.config;

import com.datasift.dropwizard.zookeeper.config.ZooKeeperConfiguration;
import com.google.common.io.Resources;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.yammer.dropwizard.config.ConfigurationFactory;
import com.yammer.dropwizard.validation.Validator;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/** Tests {@link CuratorConfiguration} */
public class CuratorConfigurationTest {

    private CuratorConfiguration config = null;

    @Before
    public void setup() throws Exception {
        config = ConfigurationFactory
                .forClass(CuratorConfiguration.class, new Validator())
                .build(new File(Resources.getResource("yaml/curator.yaml").toURI()));
    }

    @Test
    public void testZooKeeper() {
        assertThat("has ZooKeeperConfiguration",
                config.getEnsembleConfiguration(),
                instanceOf(ZooKeeperConfiguration.class));
    }

    @Test
    public void testRetryPolicy() {
        assertThat("has RetryPolicy",
                config.getRetryPolicy(),
                instanceOf(ExponentialBackoffRetry.class));
    }
}
