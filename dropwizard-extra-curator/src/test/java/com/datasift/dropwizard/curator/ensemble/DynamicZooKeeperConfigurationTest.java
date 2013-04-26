package com.datasift.dropwizard.curator.ensemble;

import com.yammer.dropwizard.config.ConfigurationFactory;
import com.yammer.dropwizard.util.Duration;
import com.yammer.dropwizard.validation.Validator;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests {@link com.datasift.dropwizard.curator.ensemble.DropwizardConfiguredZooKeeperFactory.DynamicZooKeeperConfiguration}.
 */
public class DynamicZooKeeperConfigurationTest {

    @Test
    public void parsesFullConnectionString() {
        final String hostname = "zookeeper.lan";
        final int port = 2182;
        final DropwizardConfiguredZooKeeperFactory.DynamicZooKeeperConfiguration conf
                = new DropwizardConfiguredZooKeeperFactory.DynamicZooKeeperConfiguration(
                    hostname + ":" + port, 0, true);

        assertThat("parses hostname from connection string",
                conf.getHosts(),
                is(equalTo(new String[] { hostname })));

        assertThat("parses port from connection string",
                conf.getPort(),
                is(port));
    }

    @Test
    public void parsesConnectionStringWithDefaultPort() {
        final String hostname = "zookeeper.lan";
        final int port = 2181;
        final DropwizardConfiguredZooKeeperFactory.DynamicZooKeeperConfiguration conf
                = new DropwizardConfiguredZooKeeperFactory.DynamicZooKeeperConfiguration(
                    hostname, 0, true);

        assertThat("parses hostname from connection string",
                conf.getHosts(),
                is(equalTo(new String[] { hostname })));

        assertThat("uses default port", conf.getPort(), is(port));
    }
}
