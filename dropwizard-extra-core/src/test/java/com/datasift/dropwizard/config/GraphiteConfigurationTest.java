package com.datasift.dropwizard.config;

import com.yammer.dropwizard.util.Duration;
import org.junit.Test;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/**
 * Tests {@link GraphiteConfiguration}.
 */
public class GraphiteConfigurationTest {

    @Test
    public void hasValidDefaults() {
        final GraphiteConfiguration conf = new GraphiteConfiguration();

        assertThat("default hostname is localhost",
                conf.getHost(), is("localhost"));
        assertThat("default port is ZooKeeper default",
                conf.getPort(), is(8080));
        assertThat("default prefix is empty",
                conf.getPrefix(), is(""));
        assertThat("default frequency is 1 minute",
                conf.getFrequency(), equalTo(Duration.minutes(1)));
        assertThat("disabled by default",
                conf.getEnabled(), is(false));
    }
}
