package com.datasift.dropwizard.curator.ensemble;

import io.dropwizard.setup.Environment;
import com.datasift.dropwizard.zookeeper.ZooKeeperFactory;
import org.apache.curator.utils.ZookeeperFactory;
import io.dropwizard.util.Duration;
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

    private final String name;
    private final Environment environment;

    /**
     * Initializes this factory with the {@link ZooKeeperFactory} to create {@link ZooKeeper}
     * clients from.
     *
     * @param name the name of the Curator instance creating {@link ZooKeeper} clients.
     */
    public DropwizardConfiguredZooKeeperFactory(final Environment environment, final String name) {
        this.environment = environment;
        this.name = name;
    }

    @Override
    public ZooKeeper newZooKeeper(final String connectString,
                                  final int sessionTimeout,
                                  final Watcher watcher,
                                  final boolean canBeReadOnly) throws Exception {

        return new DynamicZooKeeperFactory(connectString, sessionTimeout, canBeReadOnly)
                .build(environment, watcher, String.format("curator-%s", name));
    }

    static class DynamicZooKeeperFactory extends ZooKeeperFactory {

        DynamicZooKeeperFactory(final String connectString,
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
