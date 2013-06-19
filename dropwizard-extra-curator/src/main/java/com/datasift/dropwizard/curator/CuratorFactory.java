package com.datasift.dropwizard.curator;

import com.codahale.dropwizard.util.Duration;
import com.datasift.dropwizard.curator.ensemble.DropwizardConfiguredEnsembleProvider;
import com.datasift.dropwizard.curator.ensemble.DropwizardConfiguredZooKeeperFactory;
import com.datasift.dropwizard.curator.health.CuratorHealthCheck;
import com.datasift.dropwizard.zookeeper.ZooKeeperFactory;
import com.fasterxml.jackson.annotation.JsonCreator;
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
import java.util.Locale;

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
     */
    enum CompressionCodec {

        /**
         * GZIP compression.
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

        /**
         * Parses a {@link CompressionCodec} from the given String representation.
         *
         * @param codec the name of the codec.
         * @return the named {@link CompressionCodec} or null, if the codec doesn't exist.
         */
        @JsonCreator
        public static CompressionCodec parse(final String codec) {
            return valueOf(codec.toUpperCase(Locale.ENGLISH).replace('+', '_'));
        }
    }

    /**
     * The configuration of the ZooKeeper ensemble to use.
     */
    @JsonProperty
    @Valid
    @NotNull
    protected ZooKeeperFactory ensemble = new ZooKeeperFactory();

    /**
     * The maximum number of connection retries.
     */
    @JsonProperty
    @Min(0)
    protected int maxRetries = 1;

    /**
     * The initial delay between retries. After each retry, the delay will
     * increase exponentially.
     */
    @JsonProperty
    @NotNull
    protected Duration backOffBaseTime = Duration.seconds(1);

    /**
     * The compression scheme to use for compressed values.
     */
    @JsonProperty
    @NotNull
    protected CompressionCodec compression = CompressionCodec.GZIP;

    /**
     * @see #ensemble
     */
    public ZooKeeperFactory getEnsemble() {
        return ensemble;
    }

    /**
     * @see #backOffBaseTime
     * @see #maxRetries
     */
    public RetryPolicy getRetryPolicy() {
        return new ExponentialBackoffRetry((int) backOffBaseTime.toMilliseconds(), maxRetries);
    }

    /**
     * @see #compression
     */
    public CompressionProvider getCompressionProvider() {
        return compression.getProvider();
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
        final ZooKeeperFactory factory = getEnsemble();
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

        environment.admin().addHealthCheck(name, new CuratorHealthCheck(framework));
        environment.lifecycle().manage(new ManagedCuratorFramework(framework));

        return framework;
    }
}
