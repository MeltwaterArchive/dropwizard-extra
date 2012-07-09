package com.datasift.dropwizard.config;

import com.google.common.base.Joiner;
import com.yammer.dropwizard.util.Duration;
import org.codehaus.jackson.annotate.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;
import org.hibernate.validator.constraints.Range;

import javax.validation.constraints.NotNull;

/**
 * A {@link com.yammer.dropwizard.config.Configuration} for a ZooKeeper cluster.
 */
public class ZooKeeperConfiguration {

    @JsonProperty
    @NotEmpty
    protected String[] hosts = new String[]{ "localhost" };

    @JsonProperty
    @Range(min = 0, max = 49151)
    protected int port = 2181;

    @JsonProperty
    @NotNull
    protected Duration timeout = Duration.minutes(1);

    public String[] getHosts() {
        return hosts;
    }

    public int getPort() {
        return port;
    }

    public Duration getTimeout() {
        return timeout;
    }

    /**
     * Retrieves a formatted specification of the ZooKeeper cluster.
     *
     * The specification is formatted as: host1:port,host2:port[,hostN:port]
     *
     * @return a specification of the ZooKeeper cluster, formatted as a String
     */
    public String getQuorumSpec() {
        return Joiner
                .on(":" + port + ",")
                .skipNulls()
                .appendTo(new StringBuilder(), hosts)
                .append(':')
                .append(port)
                .toString();
    }
}
