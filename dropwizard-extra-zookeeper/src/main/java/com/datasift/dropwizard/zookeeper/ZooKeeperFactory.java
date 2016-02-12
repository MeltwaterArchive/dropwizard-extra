package com.datasift.dropwizard.zookeeper;

import io.dropwizard.util.Duration;
import io.dropwizard.validation.ValidationMethod;
import com.datasift.dropwizard.zookeeper.health.ZooKeeperHealthCheck;
import io.dropwizard.setup.Environment;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.PathUtils;
import org.hibernate.validator.constraints.NotEmpty;
import org.hibernate.validator.constraints.Range;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import java.io.IOException;

/**
 * A Factory for creating configured and managed {@link ZooKeeper} client instances.
 * <p>
 * A {@link ZooKeeperHealthCheck} will be registered for each {@link ZooKeeper} client instance that
 * checks for the existence of the configured {@link #namespace}.
 *
 * @see ZooKeeperHealthCheck
 * @see ManagedZooKeeper
 */
public class ZooKeeperFactory {

    private static final String DEFAULT_NAME = "zookeeper-default";

    /**
     * Authorization details for a ZooKeeper ensemble.
     */
    public static class Auth {

        @NotEmpty
        protected String scheme;

        @NotEmpty
        protected String id;

        /**
         * Returns the authorization scheme to use (e.g. "host").
         *
         * @return the authorization scheme to use.
         */
        @JsonProperty
        public String getScheme() {
            return scheme;
        }

        /**
         * Sets the authorization scheme to use (e.g. "host").
         *
         * @param scheme the authorization scheme to use.
         */
        @JsonProperty
        public void setScheme(final String scheme) {
            this.scheme = scheme;
        }

        /**
         * Returns the authorization id to use.
         * <p>
         * This is dependent on the authorization {@link #getScheme() scheme} being used.
         *
         * @return the scheme-specific authorization id.
         *
         * @see #getScheme()
         */
        @JsonProperty
        public String getId() {
            return id;
        }

        /**
         * Sets the authorization id to use.
         * <p>
         * This is dependent on the authorization {@link #getScheme() scheme} being used.
         *
         * @param id the scheme-specific authorization id.
         *
         * @see #setScheme(String)
         */
        @JsonProperty
        public void setId(final String id) {
            this.id = id;
        }
    }

    @NotEmpty
    protected String[] hosts = new String[]{ "localhost" };

    @Range(min = 0, max = 49151)
    protected int port = 2181;

    @Valid
    protected Auth auth = null;

    @NotEmpty
    @Pattern(regexp = "^\\/\\S*$")
    protected String namespace = "/";

    @NotNull
    protected Duration connectionTimeout = Duration.seconds(6);

    @NotNull
    protected Duration sessionTimeout = Duration.seconds(6);

    protected boolean readOnly = false;


    /**
     * Returns the hostnames of every node in the ZooKeeper quorum.
     *
     * @return the hostanmes of every node in the ZooKeeper quorum.
     */
    @JsonProperty
    public String[] getHosts() {
        return hosts;
    }

    /**
     * Sets the hostnames of every node in the ZooKeeper quorum.
     *
     * @param hosts the hostnames of every node in the ZooKeeper quorum.
     */
    @JsonProperty
    public void setHosts(final String[] hosts) {
        this.hosts = hosts;
    }

    /**
     * Returns the port to connect to every ZooKeeper node in the quorum on.
     *
     * @return the port to connect to every ZooKeeper node in the quorum on.
     */
    @JsonProperty
    public int getPort() {
        return port;
    }

    /**
     * Sets the port to connect to every ZooKeeper node in the quorum on.
     *
     * @param port the port to connect to every ZooKeeper node in the quorum on.
     */
    @JsonProperty
    public void setPort(final int port) {
        this.port = port;
    }

    /**
     * Returns authorization details to provide to this ZooKeeper connection.
     *
     * @return any authorization details to provide to ZooKeeper, or null.
     *
     * @see Auth
     */
    @JsonProperty
    public Auth getAuth() {
        return auth;
    }

    /**
     * Sets authorization details to provide to this ZooKeeper connection.
     *
     * @param auth any authorization details to provide to ZooKeeper, or null.
     *
     * @see Auth
     */
    @JsonProperty
    public void setAuth(final Auth auth) {
        this.auth = auth;
    }

    /**
     * Returns the maximum time to wait for a successful connection to a node in the quorum.
     *
     * @return the maximum time to wait for a successful connection to a node in the quorum.
     */
    @JsonProperty
    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    /**
     * Sets the maximum time to wait for a successful connection to a node in the quorum.
     *
     * @param timeout the maximum time to wait for a successful connection to a node in the quorum.
     */
    @JsonProperty
    public void setConnectionTimeout(final Duration timeout) {
        this.connectionTimeout = timeout;
    }

    /**
     * Returns the maximum time to allow a ZooKeeper session to remain idle before ending it.
     *
     * @return the maximum time to allow a ZooKeeper session to remain idle before ending it.
     */
    @JsonProperty
    public Duration getSessionTimeout() {
        return sessionTimeout;
    }

    /**
     * Sets the maximum time to allow a ZooKeeper session to remain idle before ending it.
     *
     * @param timeout the maximum time to allow a ZooKeeper session to remain idle before ending it.
     */
    @JsonProperty
    public void setSessionTimeout(final Duration timeout) {
        this.sessionTimeout = timeout;
    }

    /**
     * Returns the namespace to prepend to all paths accessed by the ZooKeeper client.
     * <p>
     * Since ZooKeeper is a shared space, this is a useful way to localise a service to a namespace.
     *
     * @return the namespace to prepend to all paths accessed by the ZooKeeper client.
     */
    @JsonProperty
    public String getNamespace() {
        return namespace;
    }

    /**
     * Sets the namespace to prepend to all paths accessed by the ZooKeeper client.
     * <p>
     * Since ZooKeeper is a shared space, this is a useful way to localise a service to a namespace.
     *
     * @param namespace the namespace to prepend to all paths accessed by the ZooKeeper client.
     */
    @JsonProperty
    public void setNamespace(final String namespace) {
        this.namespace = namespace;
    }

    /**
     * Returns whether or not this client can connect to read-only ZooKeeper instances.
     * <p>
     * During a network partition, some or all nodes in the quorum may be in a read-only state. This
     * controls whether the client may enter read-only mode during a network partition.
     *
     * @return true if the client may connect to read-only quorums, false if not.
     */
    @JsonProperty
    public boolean isReadOnly() {
        return readOnly;
    }

    /**
     * Sets whether or not this client can connect to read-only ZooKeeper instances.
     * <p>
     * During a network partition, some or all nodes in the quorum may be in a read-only state. This
     * controls whether the client may enter read-only mode during a network partition.
     *
     * @param readOnly true if the client may connect to read-only quorums, false if not.
     */
    @JsonProperty
    public void isReadOnly(final boolean readOnly) {
        this.readOnly = readOnly;
    }

    /**
     * Retrieves a formatted specification of the ZooKeeper quorum..
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

    /**
     * Validates that the ZooKeeper client namespace is a valid ZNode.
     * <p>
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

    /**
     * Builds a default {@link ZooKeeper} instance..
     * <p>
     * No {@link Watcher} will be configured for the built {@link ZooKeeper} instance. If you wish
     * to watch all events on the {@link ZooKeeper} client, use {@link #build(Environment, Watcher)}.
     *
     * @param environment the environment to build {@link ZooKeeper} instances for.
     *
     * @return a {@link ZooKeeper} client, managed and configured according to the {@code
     *         configuration}.
     *
     * @throws IOException if there is a network failure.
     */
    public ZooKeeper build(final Environment environment) throws IOException {
        return build(environment, null, DEFAULT_NAME);
    }

    /**
     * Builds a default {@link ZooKeeper} instance.
     * <p>
     * The given {@link Watcher} will be assigned to watch for all events on the {@link ZooKeeper}
     * client instance. If you wish to ignore events, use {@link #build(Environment)}.
     *
     * @param environment the environment to build {@link ZooKeeper} instances for.
     * @param watcher the watcher to handle all events that occur on the {@link ZooKeeper} client.
     *
     * @return a {@link ZooKeeper} client, managed and configured according to the {@code
     *         configuration}.
     *
     * @throws IOException if there is a network failure.
     */
    public ZooKeeper build(final Environment environment, final Watcher watcher)
            throws IOException {
        return build(environment, watcher, DEFAULT_NAME);
    }

    /**
     * Builds a named {@link ZooKeeper} instance.
     * <p>
     * No {@link Watcher} will be configured for the built {@link ZooKeeper} instance. If you wish
     * to watch all events on the {@link ZooKeeper} client, use {@link
     * #build(Environment, Watcher, String)}.
     *
     * @param environment the environment to build {@link ZooKeeper} instances for.
     * @param name the name for this {@link ZooKeeper instance}.
     *
     * @return a {@link ZooKeeper} client, managed and configured according to the {@code
     *         configuration}.
     *
     * @throws IOException if there is a network failure.
     */
    public ZooKeeper build(final Environment environment, final String name)
            throws IOException {
        return build(environment, null, name);
    }

    /**
     * Builds a named {@link ZooKeeper} instance.
     * <p>
     * The given {@link Watcher} will be assigned to watch for all events on the {@link ZooKeeper}
     * client instance. If you wish to ignore events, use {@link #build(Environment, String)}.
     *
     * @param environment the environment to build {@link ZooKeeper} instances for.
     * @param watcher the watcher to handle all events that occur on the {@link ZooKeeper} client.
     * @param name the name for this {@link ZooKeeper instance}.
     *
     * @return a {@link ZooKeeper} client, managed and configured according to the {@code
     *         configuration}.
     *
     * @throws IOException if there is a network failure.
     */
    public ZooKeeper build(final Environment environment, final Watcher watcher, final String name)
            throws IOException {

        final String quorumSpec = getQuorumSpec();
        final String namespace = getNamespace();

        final ZooKeeper client = new ZooKeeper(
                quorumSpec + namespace,
                (int) getSessionTimeout().toMilliseconds(),
                watcher,
                isReadOnly());

        final Auth auth = getAuth();
        if (auth != null) {
            client.addAuthInfo(auth.getScheme(), auth.getId().getBytes());
        }

        environment.healthChecks().register(name, new ZooKeeperHealthCheck(client, namespace));
        environment.lifecycle().manage(new ManagedZooKeeper(client));

        return client;
    }
}
