package com.datasift.dropwizard.hbase;

import io.dropwizard.jackson.Jackson;
import com.google.common.io.Resources;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.util.Duration;
import io.dropwizard.util.Size;
import org.junit.Before;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.io.File;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/**
 * Tests {@link HBaseClientFactory}.
 */
public class HBaseClientFactoryTest {

    private HBaseClientFactory factory;

    @Before
    public void setUp() throws Exception {
        final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        factory = new ConfigurationFactory<>(HBaseClientFactory.class, validator, Jackson.newObjectMapper(), "dw")
                .build(new File(Resources.getResource("yaml/hbase.yml").getFile()));
    }

    @Test
    public void hasAFlushInterval() {
        assertThat("flush interval is 1 minute",
                factory.getFlushInterval(), is(Duration.minutes(1)));
    }

    @Test
    public void hasAnIncrementBufferSize() {
        assertThat("increment buffer size is 256KB",
                factory.getIncrementBufferSize(), is(Size.kilobytes(256)));
    }

    @Test
    public void hasAMaximumConcurrentRequests() {
        assertThat("maximum concurrent requests is 1000",
                factory.getMaxConcurrentRequests(), is(1000));
    }

    @Test
    public void hasAConnectionTimeout() {
        assertThat("connection timeout is 10 seconds",
                factory.getConnectionTimeout(), is(Duration.seconds(10)));
    }

    @Test
    public void notInstrumentedWithMetrics() {
        assertThat("client is not instrumented with metrics",
                factory.isInstrumented(), is(false));
    }
}
