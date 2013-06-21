package com.datasift.dropwizard.curator.ensemble;

import com.datasift.dropwizard.zookeeper.ZooKeeperFactory;
import org.apache.curator.ensemble.EnsembleProvider;

import java.io.IOException;

/**
 * An {@link EnsembleProvider} for a fixed ensemble, configured by a {@link ZooKeeperFactory}.
 */
public class DropwizardConfiguredEnsembleProvider implements EnsembleProvider {

    private final ZooKeeperFactory factory;

    /**
     * Initializes this provider with the given {@code configuration}.
     *
     * @param factory a factory for ZooKeeper client instances.
     */
    public DropwizardConfiguredEnsembleProvider(final ZooKeeperFactory factory) {
        this.factory = factory;
    }

    @Override
    public void start() throws Exception {
        // nothing to do
    }

    @Override
    public String getConnectionString() {
        return factory.getQuorumSpec();
    }

    @Override
    public void close() throws IOException {
        // nothing to do
    }
}
