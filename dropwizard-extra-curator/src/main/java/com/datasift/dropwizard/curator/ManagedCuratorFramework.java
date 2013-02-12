package com.datasift.dropwizard.curator;

import com.netflix.curator.framework.CuratorFramework;
import com.yammer.dropwizard.lifecycle.Managed;

/**
 * Manages the lifecycle of a {@link CuratorFramework} instance.
 */
class ManagedCuratorFramework implements Managed {

    private final CuratorFramework framework;

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
