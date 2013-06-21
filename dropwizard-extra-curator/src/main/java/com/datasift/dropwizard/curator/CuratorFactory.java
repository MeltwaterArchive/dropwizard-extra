package com.datasift.dropwizard.curator;

import com.codahale.dropwizard.util.Duration;
import com.datasift.dropwizard.curator.ensemble.DropwizardConfiguredEnsembleProvider;
import com.datasift.dropwizard.curator.ensemble.DropwizardConfiguredZooKeeperFactory;
import com.datasift.dropwizard.curator.health.CuratorHealthCheck;
import com.datasift.dropwizard.zookeeper.ZooKeeperFactory;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.codahale.dropwizard.setup.Environment;
import com.netflix.curator.framework.api.CompressionProvider;
import com.netflix.curator.framework.imps.GzipCompressionProvider;
import com.netflix.curator.retry.ExponentialBackoffRetry;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * A factory for creating and managing {@link CuratorFramework} instances.
 * <p/>
 * The resulting {@link CuratorFramework} will have its lifecycle managed by the {@link Environment}
 * and will have {@link com.codahale.metrics.health.HealthCheck}s installed for the underlying ZooKeeper
 * ensemble.
 *
 * @see CuratorFramework
 */
public class CuratorFactory {

    private static final String DEFAULT_NAME = "curator-default";

    /**
     * An enumeration of the available compression codecs available for compressed entries.
     *
     * @see #getCompressionProvider()
     * @see CompressionProvider
     */
    enum CompressionCodec {

        /**
         * GZIP compression.
         *
         * @see GzipCompressionProvider
         */
        GZIP(new GzipCompressionProvider());

        final private CompressionProvider provider;

        CompressionCodec(final CompressionProvider provider) {
            this.provider = provider;
        }

        /**
         * Gets the {@link CompressionProvider} for this codec.
         *
         * @return the provider for this codec.
         */
        public CompressionProvider getProvider() {
            return provider;
        }
    }

    @Valid
    @NotNull
    protected ZooKeeperFactory ensemble = new ZooKeeperFactory();

    @Min(0)
    protected int maxRetries = 1;

    @NotNull
    protected Duration backOffBaseTime = Duration.seconds(1);

    @NotNull
    protected CompressionCodec compression = CompressionCodec.GZIP;

    /**
     * Returns a {@link ZooKeeperFactory} for the ZooKeeper ensemble to connect to.
     *
     * @return a factory for the ZooKeeper ensemble for the client.
     */
    @JsonProperty("ensemble")
    public ZooKeeperFactory getZooKeeperFactory() {
        return ensemble;
    }

    /**
     * Sets the {@link ZooKeeperFactory} for the ZooKeeper ensemble to connect to.
     *
     * @param factory the factory for the ZooKeeper ensemble for the client.
     */
    @JsonProperty("ensemble")
    public void setZooKeeperFactory(final ZooKeeperFactory factory) {
        this.ensemble = factory;
    }

    /**
     * Returns the maximum number of retries to attempt to connect to the ensemble.
     *
     * @return the maximum number of connection attempts.
     */
    @JsonProperty
    public int getMaxRetries() {
        return maxRetries;
    }

    /**
     * Sets the maximum number of retries to attempt to connect to the ensemble.
     *
     * @param maxRetries the maximum number of connection attempts.
     */
    @JsonProperty
    public void setMaxRetries(final int maxRetries) {
        this.maxRetries = maxRetries;
    }

    /**
     * Returns the initial time to wait before retrying a failed connection.
     * <p/>
     * Subsequent retries will wait an exponential amount of time more than this.
     *
     * @return the initial time to wait before trying to connect again.
     */
    @JsonProperty
    public Duration getBackOffBaseTime() {
        return backOffBaseTime;
    }

    /**
     * Sets the initial time to wait before retrying a failed connection.
     * <p/>
     * Subsequent retries will wait an exponential amount of time more than this.
     *
     * @param backOffBaseTime the initial time to wait before trying to connect again.
     */
    @JsonProperty
    public void setBackOffBaseTime(final Duration backOffBaseTime) {
        this.backOffBaseTime = backOffBaseTime;
    }

    /**
     * Returns a {@link RetryPolicy} for handling failed connection attempts.
     * <p/>
     * Always configures an {@link ExponentialBackoffRetry} based on the {@link #getMaxRetries()
     * maximum retries} and {@link #getBackOffBaseTime() initial back-off} configured.
     *
     * @return a {@link RetryPolicy} for handling failed connection attempts.
     *
     * @see #getMaxRetries()
     * @see #getBackOffBaseTime()
     */
    public RetryPolicy getRetryPolicy() {
        return new ExponentialBackoffRetry((int) backOffBaseTime.toMilliseconds(), maxRetries);
    }

    /**
     * Returns the {@link CompressionCodec} to compress values with.
     *
     * @return the compression codec to compress values with.
     *
     * @see CompressionCodec
     */
    @JsonProperty("compression")
    public CompressionCodec getCompressionCodec() {
        return compression;
    }

    /**
     * Sets a {@link CompressionCodec} to compress values with.
     *
     * @param codec the compression codec to compress values with.
     *
     * @see CompressionCodec
     */
    @JsonProperty("compression")
    public void setCompressionCodec(final CompressionCodec codec) {
        this.compression = codec;
    }

    /**
     * Returns a {@link CompressionProvider} to compress values with.
     *
     * @return the compression provider used to compress values.
     *
     * @see CompressionCodec
     */
    public CompressionProvider getCompressionProvider() {
        return getCompressionCodec().getProvider();
    }

    /**
     * Builds a default {@link CuratorFramework} for the given {@link Environment}.
     *
     * @param environment the {@link Environment} to build the {@link CuratorFramework} for.
     *
     * @return a {@link CuratorFramework} instance, managed and configured.
     */
    public CuratorFramework build(final Environment environment) {
        return build(environment, DEFAULT_NAME);
    }

    /**
     * Builds a {@link CuratorFramework} instance with the given {@code name} for an {@link
     * Environment}.
     *
     * @param environment the {@link Environment} to build the {@link CuratorFramework} for.
     * @param name the name for the {@link CuratorFramework} instance.
     *
     * @return a {@link CuratorFramework} instance, managed and configured.
     */
    public CuratorFramework build(final Environment environment, final String name) {
        final ZooKeeperFactory factory = getZooKeeperFactory();
        final CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
                .zookeeperFactory(new DropwizardConfiguredZooKeeperFactory(environment, name))
                .ensembleProvider(new DropwizardConfiguredEnsembleProvider(factory))
                .connectionTimeoutMs((int) factory.getConnectionTimeout().toMilliseconds())
                .threadFactory(new ThreadFactoryBuilder().setNameFormat(name + "-%d").build())
                .sessionTimeoutMs((int) factory.getSessionTimeout().toMilliseconds())
                .namespace(factory.getNamespace())
                .compressionProvider(getCompressionProvider())
                .retryPolicy(getRetryPolicy())
                .canBeReadOnly(factory.isReadOnly());

        // add optional auth details
        final ZooKeeperFactory.Auth auth = factory.getAuth();
        if (auth != null) {
            builder.authorization(auth.getScheme(), auth.getId().getBytes());
        }

        final CuratorFramework framework = builder.build();

        environment.healthChecks().register(name, new CuratorHealthCheck(framework));
        environment.lifecycle().manage(new ManagedCuratorFramework(framework));

        return framework;
    }
}
