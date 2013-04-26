package com.datasift.dropwizard.curator.ensemble;

import com.datasift.dropwizard.zookeeper.ZooKeeperFactory;
import com.datasift.dropwizard.zookeeper.config.ZooKeeperConfiguration;
import com.netflix.curator.utils.ZookeeperFactory;
import com.yammer.dropwizard.util.Duration;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Provides integration for Dropwizard's ZooKeeper functionality with Curator.
 * <p/>
 * This ensures that {@link ZooKeeper} instances created by Curator integrate properly with the
 * Dropwizard application life-cycle.
 */
public class DropwizardConfiguredZooKeeperFactory implements ZookeeperFactory {

    private static final Pattern PORT_PATTERN = Pattern.compile(":(\\d+)");

    private final ZooKeeperFactory factory;
    private final String name;

    /**
     * Initializes this factory with the {@link ZooKeeperFactory} to create {@link ZooKeeper}
     * clients from.
     *
     * @param factory the factory to create {@link ZooKeeper} instances from.
     * @param name the name of the Curator instance creating {@link ZooKeeper} clients.
     */
    public DropwizardConfiguredZooKeeperFactory(final ZooKeeperFactory factory, final String name) {
        this.factory = factory;
        this.name = name;
    }

    @Override
    public ZooKeeper newZooKeeper(final String connectString,
                                  final int sessionTimeout,
                                  final Watcher watcher,
                                  final boolean canBeReadOnly) throws Exception {

        return factory.build(
                new DynamicZooKeeperConfiguration(connectString, sessionTimeout, canBeReadOnly),
                watcher,
                String.format("curator-%s", name));
    }

    static class DynamicZooKeeperConfiguration extends ZooKeeperConfiguration {

        DynamicZooKeeperConfiguration(final String connectString,
                                      final int sessionTimeout,
                                      final boolean canBeReadOnly) {
            final int idx = connectString.indexOf('/');
            final int hostLength = idx == -1 ? connectString.length() : idx;
            final String authority = connectString.substring(0, hostLength);
            final Matcher matcher = PORT_PATTERN.matcher(authority);
            this.port = matcher.find() ? Integer.parseInt(matcher.group(1)) : port;
            this.hosts = matcher.replaceAll("").split(",");
            this.namespace = idx == -1 ? "/" : connectString.substring(idx);
            this.sessionTimeout = Duration.milliseconds(sessionTimeout);
            this.readOnly = canBeReadOnly;
        }
    }
}
