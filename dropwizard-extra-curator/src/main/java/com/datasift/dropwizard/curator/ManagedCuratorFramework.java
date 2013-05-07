package com.datasift.dropwizard.curator;

import com.netflix.curator.framework.CuratorFramework;
import com.codahale.dropwizard.lifecycle.Managed;

/**
 * Manages the lifecycle of a {@link CuratorFramework} instance.
 */
class ManagedCuratorFramework implements Managed {

    private final CuratorFramework framework;

    /**
     * Manage the given {@link CuratorFramework} instance.
     *
     * @param framework the Curator instance to manage.
     */
    public ManagedCuratorFramework(final CuratorFramework framework) {
        this.framework = framework;
    }

    @Override
    public void start() throws Exception {
        framework.start();
    }

    @Override
    public void stop() throws Exception {
        framework.close();
    }
}
