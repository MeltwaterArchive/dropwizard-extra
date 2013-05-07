package com.datasift.dropwizard.curator.ensemble;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

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
