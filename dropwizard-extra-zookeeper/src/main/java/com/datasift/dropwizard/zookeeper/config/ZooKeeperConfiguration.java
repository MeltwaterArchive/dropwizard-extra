package com.datasift.dropwizard.zookeeper.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.codahale.dropwizard.util.Duration;
import com.codahale.dropwizard.validation.ValidationMethod;
import org.apache.zookeeper.common.PathUtils;
import org.hibernate.validator.constraints.NotEmpty;
import org.hibernate.validator.constraints.Range;

import javax.validation.constraints.NotNull;

/**
 * Configuration for a ZooKeeper cluster.
 */
public class ZooKeeperConfiguration {

    /**
     * Authorization details for a ZooKeeper ensemble.
     */
    public static class Auth {

        /**
         * The authorization scheme to use (e.g. "host").
         */
        @JsonProperty
        @NotEmpty
        protected String scheme;

        /**
         * The authorization id to use (e.g. "foo.example.com").
         * <p/>
         * This is dependent on the authorization {@link #scheme} being used.
         */
        @JsonProperty
        @NotEmpty
        protected String id;

        /**
         * @see #scheme
         */
        public String getScheme() {
            return scheme;
        }

        /**
         * @see #id
         */
        public byte[] getId() {
            return id.getBytes();
        }
    }

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
     * Authorization details to provide to this ZooKeeper connection.
     *
     * @see Auth
     */
    @JsonProperty
    protected Auth auth = null;

    /**
     * Namespace to to prepend to all paths accessed by the ZooKeeper client.
     * <p/>
     * Since ZooKeeper is a shared space, this is a useful way to localise a service to a namespace.
     */
    @JsonProperty
    @NotEmpty
    protected String namespace = "/";

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
     * Whether or not this client can connect to read-only ZooKeeper instances.
     * <p/>
     * During a network partition, some or all nodes in the quorum may be in a read-only state. This
     * controls whether the client may enter read-only mode during a network partition.
     */
    @JsonProperty
    protected boolean readOnly = false;

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
     * @see Auth#auth
     */
    public Auth getAuth() {
        return auth;
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
     * @see ZooKeeperConfiguration#namespace
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * Retrieves a formatted specification of the ZooKeeper quorum..
     * <p/>
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

    /**
     * Whether the ZooKeeper client may enter read-only mode during a network partition.
     *
     * @return true if the client may enter read-only mode; false otherwise.
     */
    public boolean canBeReadOnly() {
        return readOnly;
    }

    /**
     * Validates that the ZooKeeper client namespace is a valid ZNode.
     * <p/>
     * Note: this validation doesn't ensure that the ZNode exists, just that it is valid.
     *
     * @return true if the namespace is a valid ZNode; false if it is not.
     */
    @ValidationMethod(message = "namespace must be a valid ZooKeeper ZNode")
    public boolean isNamespaceValid() {
        try {
            PathUtils.validatePath(namespace);
        } catch (final IllegalArgumentException e) {
            return false;
        }
        return true;
    }
}
