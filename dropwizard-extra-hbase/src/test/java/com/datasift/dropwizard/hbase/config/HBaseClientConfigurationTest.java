package com.datasift.dropwizard.hbase.config;

import com.google.common.io.Resources;
import com.yammer.dropwizard.config.ConfigurationFactory;
import com.yammer.dropwizard.util.Duration;
import com.yammer.dropwizard.util.Size;
import com.yammer.dropwizard.validation.Validator;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/**
 * Tests {@link HBaseClientConfiguration}.
 */
public class HBaseClientConfigurationTest {

    private HBaseClientConfiguration conf;

    @Before
    public void setUp() throws Exception {
        conf = ConfigurationFactory
                .forClass(HBaseClientConfiguration.class, new Validator())
                .build(new File(Resources.getResource("yaml/hbase.yml").getFile()));
    }

    @Test
    public void hasAFlushInterval() {
        assertThat("flush interval is 1 minute",
                conf.getFlushInterval(), is(Duration.minutes(1)));
    }

    @Test
    public void hasAnIncrementBufferSize() {
        assertThat("increment buffer size is 256KB",
                conf.getIncrementBufferSize(), is(Size.kilobytes(256)));
    }

    @Test
    public void hasAMaximumConcurrentRequests() {
        assertThat("maximum concurrent requests is 1000",
                conf.getMaxConcurrentRequests(), is(1000));
    }

    @Test
    public void hasAConnectionTimeout() {
        assertThat("connection timeout is 10 seconds",
                conf.getConnectionTimeout(), is(Duration.seconds(10)));
    }

    @Test
    public void notInstrumentedWithMetrics() {
        assertThat("client is not instrumented with metrics",
                conf.isInstrumented(), is(false));
    }
}
