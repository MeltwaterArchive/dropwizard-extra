package com.datasift.dropwizard.zookeeper;

import io.dropwizard.jackson.Jackson;
import com.google.common.io.Resources;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.util.Duration;
import org.junit.Before;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.io.File;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests {@link ZooKeeperConfiguration}.
 */
public class ZooKeeperFactoryTest {

    ZooKeeperFactory config = null;

    @Before
    public void setup() throws Exception {
        final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        config = new ConfigurationFactory<>(ZooKeeperFactory.class, validator, Jackson.newObjectMapper(), "dw")
                .build(new File(Resources.getResource("yaml/zookeeper.yaml").toURI()));
    }

    @Test
    public void hasValidDefaults() {
        final ZooKeeperFactory conf = new ZooKeeperFactory();

        assertThat("default hostname is localhost",
                conf.getHosts(),
                hasItemInArray("localhost"));

        assertThat("default port is ZooKeeper default",
                conf.getPort(),
                is(2181));

        assertThat("default namespace is ZooKeeper root",
                conf.getNamespace(),
                is("/"));

        assertThat("default connection timeout is 6 seconds",
                conf.getConnectionTimeout(),
                equalTo(Duration.seconds(6)));

        assertThat("default session timeout is 6 seconds",
                conf.getSessionTimeout(),
                equalTo(Duration.seconds(6)));
    }

    @Test
    public void quorumSpecForOneHost() {
        final ZooKeeperFactory conf = new ZooKeeperFactory();
        assertThat("quorum spec is correct for single host",
                conf.getQuorumSpec(),
                is("localhost:2181"));
    }

    @Test
    public void quorumSpecForMultipleHosts() {
        final ZooKeeperFactory conf = mock(ZooKeeperFactory.class);
        when(conf.getHosts()).thenReturn(new String[] { "remote1", "remote2" });
        when(conf.getPort()).thenReturn(2181);
        when(conf.getQuorumSpec()).thenCallRealMethod();

        assertThat("quorum spec is correct for multiple hosts",
                conf.getQuorumSpec(),
                is("remote1:2181,remote2:2181"));
    }

    @Test
    public void namespacePath() {
        final ZooKeeperFactory conf = new ZooKeeperFactory();
        assertThat("namespace represents a valid path",
                conf.getNamespace(),
                is("/"));

        assertThat("namespace String represents a valid path",
                conf.getNamespace(),
                is("/"));
    }

    @Test
    public void parsedConfig() {
        assertThat("contains hosts",
                config.getHosts(),
                is(new String[] { "test1", "test2" }));

        assertThat("parses port",
                config.getPort(),
                is(2182));

        assertThat("parses namespace",
                config.getNamespace(),
                is("/test"));

        assertThat("parses connection timeout",
                config.getConnectionTimeout(),
                is(Duration.seconds(10)));

        assertThat("parses session timeout",
                config.getSessionTimeout(),
                is(Duration.seconds(30)));
    }
}
