package com.datasift.dropwizard.curator.config;

import com.datasift.dropwizard.zookeeper.config.ZooKeeperConfiguration;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.api.CompressionProvider;
import com.netflix.curator.framework.imps.GzipCompressionProvider;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.yammer.dropwizard.util.Duration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.Locale;

/**
 * A configuration for a {@link com.netflix.curator.framework.CuratorFramework} instance.
 */
public class CuratorConfiguration {

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
    @NotNull
    protected ZooKeeperConfiguration ensemble = new ZooKeeperConfiguration();

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
    public ZooKeeperConfiguration getEnsembleConfiguration() {
        return ensemble;
    }

    /**
     * @see #retryPolicy
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
}
