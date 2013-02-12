package com.datasift.dropwizard.curator.ensemble;

import com.datasift.dropwizard.zookeeper.config.ZooKeeperConfiguration;
import com.netflix.curator.ensemble.EnsembleProvider;

import java.io.IOException;

/**
 * An {@link EnsembleProvider} for a fixed ensemble, configured by a {@link ZooKeeperConfiguration}.
 */
public class DropwizardConfiguredEnsembleProvider implements EnsembleProvider {

    private final ZooKeeperConfiguration configuration;

    public DropwizardConfiguredEnsembleProvider(final ZooKeeperConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void start() throws Exception {
        // nothing to do
    }

    @Override
    public String getConnectionString() {
        return configuration.getQuorumSpec();
    }

    @Override
    public void close() throws IOException {
        // nothing to do
    }
}
