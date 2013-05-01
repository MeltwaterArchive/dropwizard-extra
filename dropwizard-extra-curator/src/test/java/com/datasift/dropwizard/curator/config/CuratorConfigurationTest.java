package com.datasift.dropwizard.curator.config;

import com.codahale.dropwizard.configuration.ConfigurationFactory;
import com.datasift.dropwizard.zookeeper.config.ZooKeeperConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import org.junit.Before;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.io.File;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/** Tests {@link CuratorConfiguration} */
public class CuratorConfigurationTest {

    private CuratorConfiguration config = null;

    @Before
    public void setup() throws Exception {
        final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        config = new ConfigurationFactory<>(CuratorConfiguration.class, validator, new ObjectMapper(), "dw")
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
