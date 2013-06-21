package com.datasift.dropwizard.curator;

import com.codahale.dropwizard.configuration.ConfigurationFactory;
import com.codahale.dropwizard.jackson.Jackson;
import com.datasift.dropwizard.zookeeper.ZooKeeperFactory;
import com.google.common.io.Resources;
import com.netflix.curator.framework.api.CompressionProvider;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import org.junit.Before;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.io.File;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/** Tests {@link CuratorConfiguration} */
public class CuratorFactoryTest {

    private CuratorFactory factory = null;

    @Before
    public void setup() throws Exception {
        final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        factory = new ConfigurationFactory<>(CuratorFactory.class, validator, Jackson.newObjectMapper(), "dw")
                .build(new File(Resources.getResource("yaml/curator.yaml").toURI()));
    }

    @Test
    public void testZooKeeper() {
        assertThat("has ZooKeeperConfiguration",
                factory.getZooKeeperFactory(),
                instanceOf(ZooKeeperFactory.class));
    }

    @Test
    public void testRetryPolicy() {
        assertThat("has RetryPolicy",
                factory.getRetryPolicy(),
                instanceOf(ExponentialBackoffRetry.class));
    }

    @Test
    public void testCompressionCodec() {
        assertThat("has CompressionCodec",
                factory.getCompressionCodec(),
                is(CuratorFactory.CompressionCodec.GZIP));
    }

    @Test
    public void testCompressionProvider() {
        assertThat("supplied CompressionProvider",
                factory.getCompressionProvider(),
                instanceOf(CompressionProvider.class));
    }
}
