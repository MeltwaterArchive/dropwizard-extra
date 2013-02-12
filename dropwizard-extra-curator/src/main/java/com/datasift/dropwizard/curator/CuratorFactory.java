package com.datasift.dropwizard.curator;

import com.datasift.dropwizard.curator.config.CuratorConfiguration;
import com.datasift.dropwizard.curator.ensemble.DropwizardConfiguredEnsembleProvider;
import com.datasift.dropwizard.curator.ensemble.DropwizardConfiguredZooKeeperFactory;
import com.datasift.dropwizard.curator.health.CuratorHealthCheck;
import com.datasift.dropwizard.zookeeper.ZooKeeperFactory;
import com.datasift.dropwizard.zookeeper.config.ZooKeeperConfiguration;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.yammer.dropwizard.config.Environment;

import java.util.concurrent.ThreadFactory;

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
     * CuratorConfiguration}.
     *
     * @param configuration the {@link CuratorConfiguration} for the ensemble
     *                      to configure the {@link CuratorFramework} instance
     *                      for.
     * @return a {@link CuratorFramework} instance, managed and configured
     *         according to the {@code configuration}.
     */
    public CuratorFramework build(final CuratorConfiguration configuration) {
        return build(configuration, "default");
    }

    /**
     * Builds a {@link CuratorFramework} instance from the given {@link
     * CuratorConfiguration} with the given {@code name}.
     *
     * @param configuration the {@link CuratorConfiguration} for the ensemble
     *                      to configure the {@link CuratorFramework} instance
     *                      for.
     * @param name the name for the {@link CuratorFramework} instance.
     * @return a {@link CuratorFramework} instance, managed and configured
     *         according to the {@code configuration}.
     */
    public CuratorFramework build(final CuratorConfiguration configuration,
                                  final String name) {
        final ZooKeeperConfiguration zkConfiguration = configuration.getEnsembleConfiguration();
        final ZooKeeperFactory factory = new ZooKeeperFactory(environment);
        final CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
                .zookeeperFactory(new DropwizardConfiguredZooKeeperFactory(factory, name))
                .ensembleProvider(new DropwizardConfiguredEnsembleProvider(zkConfiguration))
                .connectionTimeoutMs((int) zkConfiguration.getConnectionTimeout().toMilliseconds())
                .threadFactory(new ThreadFactoryBuilder().setNameFormat(name + "-%d").build())
                .sessionTimeoutMs((int) zkConfiguration.getSessionTimeout().toMilliseconds())
                .namespace(zkConfiguration.getNamespace())
                .compressionProvider(configuration.getCompressionProvider())
                .retryPolicy(configuration.getRetryPolicy())
                .canBeReadOnly(zkConfiguration.canBeReadOnly());

        // add optional auth details
        final ZooKeeperConfiguration.Auth auth = zkConfiguration.getAuth();
        if (auth != null) {
            builder.authorization(auth.getScheme(), auth.getId());
        }

        final CuratorFramework framework = builder.build();

        environment.addHealthCheck(new CuratorHealthCheck(framework, name));
        environment.manage(new ManagedCuratorFramework(framework));

        return framework;
    }
}
