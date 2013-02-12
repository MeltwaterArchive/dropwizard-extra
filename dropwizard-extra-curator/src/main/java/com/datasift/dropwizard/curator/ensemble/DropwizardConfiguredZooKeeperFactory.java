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
 * This furnishes all {@link ZooKeeper} clients that Curator creates with all
 * the integration that Dropwizard provides (e.g. metrics).
 */
public class DropwizardConfiguredZooKeeperFactory implements ZookeeperFactory {

    public static final Pattern PORT_PATTERN = Pattern.compile(":(\\d+)");

    private final ZooKeeperFactory factory;
    private final String name;

    public DropwizardConfiguredZooKeeperFactory(final ZooKeeperFactory factory,
                                                final String name) {
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
            final String authority = connectString.substring(0, idx == -1 ? connectString.length() : idx);
            final Matcher matcher = PORT_PATTERN.matcher(authority);
            this.hosts = matcher.replaceAll("").split(",");
            this.port = Integer.parseInt(matcher.reset().group(1));
            this.namespace = idx == -1 ? "/" : connectString.substring(idx);
            this.sessionTimeout = Duration.milliseconds(sessionTimeout);
            this.readOnly = canBeReadOnly;
        }
    }
}
