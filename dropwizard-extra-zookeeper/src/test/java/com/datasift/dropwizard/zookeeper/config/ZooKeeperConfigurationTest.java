package com.datasift.dropwizard.zookeeper.config;

import com.datasift.dropwizard.zookeeper.util.ZNode;
import com.yammer.dropwizard.util.Duration;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests {@link ZooKeeperConfiguration}.
 */
public class ZooKeeperConfigurationTest {

    @Test
    public void hasValidDefaults() {
        final ZooKeeperConfiguration conf = new ZooKeeperConfiguration();

        assertThat("default hostname is localhost",
                conf.getHosts(), hasItemInArray("localhost"));
        assertThat("default port is ZooKeeper default",
                conf.getPort(), is(2181));
        assertThat("default namespace is ZooKeeper root",
                conf.getNamespace(), is(new ZNode("/")));
        assertThat("default connection timeout is 6 seconds",
                conf.getConnectionTimeout(), equalTo(Duration.seconds(6)));
        assertThat("default session timeout is 6 seconds",
                conf.getSessionTimeout(), equalTo(Duration.seconds(6)));
    }

    @Test
    public void quorumSpecForOneHost() {
        final ZooKeeperConfiguration conf = new ZooKeeperConfiguration();
        assertThat("quorum spec is correct for single host",
                conf.getQuorumSpec(), is("localhost:2181"));
    }

    @Test
    public void quorumSpecForMultipleHosts() {
        final ZooKeeperConfiguration conf = mock(ZooKeeperConfiguration.class);
        when(conf.getHosts()).thenReturn(new String[] { "remote1", "remote2" });
        when(conf.getPort()).thenReturn(2181);
        when(conf.getQuorumSpec()).thenCallRealMethod();

        assertThat("quorum spec is correct for multiple hosts",
                conf.getQuorumSpec(), is("remote1:2181,remote2:2181"));
    }

    @Test
    public void namespacePath() {
        final ZooKeeperConfiguration conf = mock(ZooKeeperConfiguration.class);
        when(conf.getNamespace()).thenReturn(new ZNode("/"));
        assertThat("namespace represents a valid path", conf.getNamespace(), is(new ZNode("/")));
        assertThat("namespace String represents a valid path",
                conf.getNamespace().toString(), is("/"));
    }
}
