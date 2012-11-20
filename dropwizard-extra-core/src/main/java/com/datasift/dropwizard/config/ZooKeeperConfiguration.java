package com.datasift.dropwizard.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.yammer.dropwizard.util.Duration;
import org.hibernate.validator.constraints.NotEmpty;
import org.hibernate.validator.constraints.Range;

import javax.validation.constraints.NotNull;

/**
 * A {@link com.yammer.dropwizard.config.Configuration} for a ZooKeeper cluster.
 */
public class ZooKeeperConfiguration {

    /**
     * Hostnames of every node in the ZooKeeper quorum.
     */
    @JsonProperty
    @NotEmpty
    protected String[] hosts = new String[]{ "localhost" };

    /**
     * Port to connect to every ZooKeeper node in the quorum on.
     */
    @JsonProperty
    @Range(min = 0, max = 49151)
    protected int port = 2181;

    /**
     * Maximum time to wait for a successful connection to a node in the quorum.
     */
    @JsonProperty
    @NotNull
    protected Duration connectionTimeout = Duration.seconds(6);

    /**
     * Maximum time to allow a ZooKeeper session to remain idle before ending it.
     */
    @JsonProperty
    @NotNull
    protected Duration sessionTimeout = Duration.seconds(6);

    /**
     * @see ZooKeeperConfiguration#hosts
     */
    public String[] getHosts() {
        return hosts;
    }

    /**
     * @see ZooKeeperConfiguration#port
     */
    public int getPort() {
        return port;
    }

    /**
     * @see ZooKeeperConfiguration#connectionTimeout
     */
    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    /**
     * @see ZooKeeperConfiguration#sessionTimeout
     */
    public Duration getSessionTimeout() {
        return sessionTimeout;
    }

    /**
     * Retrieves a formatted specification of the ZooKeeper quroum.
     * <p>
     * The specification is formatted as: host1:port,host2:port[,hostN:port]
     *
     * @return a specification of the ZooKeeper quorum, formatted as a String
     */
    public String getQuorumSpec() {
        return Joiner
                .on(":" + getPort() + ",")
                .skipNulls()
                .appendTo(new StringBuilder(), getHosts())
                .append(':')
                .append(getPort())
                .toString();
    }
}
