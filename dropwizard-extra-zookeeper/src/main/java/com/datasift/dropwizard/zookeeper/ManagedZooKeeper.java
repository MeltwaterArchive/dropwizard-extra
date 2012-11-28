package com.datasift.dropwizard.zookeeper;

import com.yammer.dropwizard.lifecycle.Managed;
import org.apache.zookeeper.ZooKeeper;
import org.eclipse.jetty.util.component.LifeCycle;

/**
 * Manages the lifecycle of a {@link ZooKeeper} client instance.
 */
public class ManagedZooKeeper implements Managed{

    private final ZooKeeper client;

    /**
     * Manage the given {@link ZooKeeper} client instance.
     *
     * @param client the client to manage.
     */
    public ManagedZooKeeper(final ZooKeeper client) {
        this.client = client;
    }

    /**
     * Start the {@link ZooKeeper} lifecycle.
     * <p/>
     * Since {@link ZooKeeper} instances connect to the ensemble in a background thread during
     * construction, this is a no-op.
     *
     * @throws Exception if an error occurs during lifecycle start.
     */
    @Override
    public void start() throws Exception {
        // already started, nothing to do
    }

    /**
     * Shuts down the managed {@link ZooKeeper} client instance.
     *
     * @throws Exception
     */
    @Override
    public void stop() throws Exception {
        client.close();
    }
}
