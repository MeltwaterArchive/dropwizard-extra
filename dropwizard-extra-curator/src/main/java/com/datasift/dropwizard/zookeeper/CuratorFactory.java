package com.datasift.dropwizard.zookeeper;

import com.datasift.dropwizard.zookeeper.config.ZooKeeperConfiguration;
import com.datasift.dropwizard.zookeeper.ensemble.DropwizardConfiguredEnsembleProvider;
import com.datasift.dropwizard.zookeeper.health.CuratorHealthCheck;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.yammer.dropwizard.config.Environment;

/**
 * A factory for creating and managing {@link CuratorFramework} instances.
 * <p>
 * The resulting {@link CuratorFramework} will have its lifecycle managed by the
 * {@link Environment} and will have {@link com.yammer.metrics.core.HealthCheck}s
 * installed for the underlying ZooKeeper ensemble.
 *
 * @see CuratorFramework
 */
public class CuratorFactory {

    private final Environment environment;

    /**
     * Creates a new {@link CuratorFactory} instance for the given {@link
     * Environment}.
     *
     * @param environment the {@link Environment} instance to build {@link
     *                    CuratorFramework} instances for.
     */
    public CuratorFactory(final Environment environment) {
        this.environment = environment;
    }

    /**
     * Builds a default {@link CuratorFramework} instance from the given {@link
     * ZooKeeperConfiguration}.
     *
     * @param configuration the {@link ZooKeeperConfiguration} for the ensemble
     *                      to configure the {@link CuratorFramework} instance
     *                      for.
     * @return a {@link CuratorFramework} instance, managed and configured
     *         according to the {@code configuration}.
     */
    public CuratorFramework build(final ZooKeeperConfiguration configuration) {
        return build(configuration, "default");
    }

    /**
     * Builds a {@link CuratorFramework} instance from the given {@link
     * ZooKeeperConfiguration} with the given {@code name}.
     *
     * @param configuration the {@link ZooKeeperConfiguration} for the ensemble
     *                      to configure the {@link CuratorFramework} instance
     *                      for.
     * @param name the name for the {@link CuratorFramework} instance.
     * @return a {@link CuratorFramework} instance, managed and configured
     *         according to the {@code configuration}.
     */
    public CuratorFramework build(final ZooKeeperConfiguration configuration,
                                  final String name) {
        final CuratorFramework framework = CuratorFrameworkFactory.builder()
                .ensembleProvider(new DropwizardConfiguredEnsembleProvider(configuration))
                .connectionTimeoutMs((int) configuration.getConnectionTimeout().toMilliseconds())
                .sessionTimeoutMs((int) configuration.getSessionTimeout().toMilliseconds())
                .namespace(configuration.getNamespace().toString())
                .build();

        environment.addHealthCheck(new CuratorHealthCheck(framework, name));
        environment.manage(new ManagedCuratorFramework(framework));

        return framework;
    }

}
