package com.datasift.dropwizard.curator.ensemble;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

/**
 * Tests {@link DropwizardConfiguredZooKeeperFactory.DynamicZooKeeperFactory}.
 */
public class DynamicZooKeeperConfigurationTest {

    @Test
    public void parsesFullConnectionString() {
        final String hostname = "zookeeper.lan";
        final int port = 2182;
        final DropwizardConfiguredZooKeeperFactory.DynamicZooKeeperFactory factory
                = new DropwizardConfiguredZooKeeperFactory.DynamicZooKeeperFactory(
                    hostname + ":" + port, 0, true);

        assertThat("parses hostname from connection string",
                factory.getHosts(),
                is(equalTo(new String[] { hostname })));

        assertThat("parses port from connection string",
                factory.getPort(),
                is(port));
    }

    @Test
    public void parsesConnectionStringWithDefaultPort() {
        final String hostname = "zookeeper.lan";
        final int port = 2181;
        final DropwizardConfiguredZooKeeperFactory.DynamicZooKeeperFactory conf
                = new DropwizardConfiguredZooKeeperFactory.DynamicZooKeeperFactory(
                    hostname, 0, true);

        assertThat("parses hostname from connection string",
                conf.getHosts(),
                is(equalTo(new String[] { hostname })));

        assertThat("uses default port", conf.getPort(), is(port));
    }
}
