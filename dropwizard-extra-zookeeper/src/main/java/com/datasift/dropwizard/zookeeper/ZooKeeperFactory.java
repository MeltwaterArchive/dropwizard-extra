package com.datasift.dropwizard.zookeeper;

import com.codahale.dropwizard.util.Duration;
import com.codahale.dropwizard.validation.ValidationMethod;
import com.datasift.dropwizard.zookeeper.health.ZooKeeperHealthCheck;
import com.codahale.dropwizard.setup.Environment;
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
 * <p/>
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

        /**
         * The authorization scheme to use (e.g. "host").
         */
        @NotEmpty
        protected String scheme;

        /**
         * The authorization id to use (e.g. "foo.example.com").
         * <p/>
         * This is dependent on the authorization {@link #scheme} being used.
         */
        @NotEmpty
        protected String id;

        /**
         * @see #scheme
         */
        @JsonProperty
        public String getScheme() {
            return scheme;
        }

        @JsonProperty
        public void setScheme(final String scheme) {
            this.scheme = scheme;
        }

        /**
         * @see #id
         */
        @JsonProperty
        public String getId() {
            return id;
        }

        @JsonProperty
        public void setId(final String id) {
            this.id = id;
        }
    }

    /**
     * Hostnames of every node in the ZooKeeper quorum.
     */
    @NotEmpty
    protected String[] hosts = new String[]{ "localhost" };

    /**
     * Port to connect to every ZooKeeper node in the quorum on.
     */
    @Range(min = 0, max = 49151)
    protected int port = 2181;

    /**
     * Authorization details to provide to this ZooKeeper connection.
     *
     * @see Auth
     */
    @Valid
    protected Auth auth = null;

    /**
     * Namespace to to prepend to all paths accessed by the ZooKeeper client.
     * <p/>
     * Since ZooKeeper is a shared space, this is a useful way to localise a service to a namespace.
     */
    @NotEmpty
    @Pattern(regexp = "^\\/\\S*$")
    protected String namespace = "/";

    /**
     * Maximum time to wait for a successful connection to a node in the quorum.
     */
    @NotNull
    protected Duration connectionTimeout = Duration.seconds(6);

    /**
     * Maximum time to allow a ZooKeeper session to remain idle before ending it.
     */
    @NotNull
    protected Duration sessionTimeout = Duration.seconds(6);

    /**
     * Whether or not this client can connect to read-only ZooKeeper instances.
     * <p/>
     * During a network partition, some or all nodes in the quorum may be in a read-only state. This
     * controls whether the client may enter read-only mode during a network partition.
     */
    protected boolean readOnly = false;

    /**
     * @see #hosts
     */
    @JsonProperty
    public String[] getHosts() {
        return hosts;
    }

    @JsonProperty
    public void setHosts(final String[] hosts) {
        this.hosts = hosts;
    }

    /**
     * @see #port
     */
    @JsonProperty
    public int getPort() {
        return port;
    }

    @JsonProperty
    public void setPort(final int port) {
        this.port = port;
    }

    /**
     * @see Auth#auth
     */
    @JsonProperty
    public Auth getAuth() {
        return auth;
    }

    @JsonProperty
    public void setAuth(final Auth auth) {
        this.auth = auth;
    }

    /**
     * @see #connectionTimeout
     */
    @JsonProperty
    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    @JsonProperty
    public void setConnectionTimeout(final Duration timeout) {
        this.connectionTimeout = timeout;
    }

    /**
     * @see #sessionTimeout
     */
    @JsonProperty
    public Duration getSessionTimeout() {
        return sessionTimeout;
    }

    @JsonProperty
    public void setSessionTimeout(final Duration timeout) {
        this.sessionTimeout = timeout;
    }

    /**
     * @see #namespace
     */
    @JsonProperty
    public String getNamespace() {
        return namespace;
    }

    @JsonProperty
    public void setNamespace(final String namespace) {
        this.namespace = namespace;
    }

    /**
     * Whether the ZooKeeper client may enter read-only mode during a network partition.
     *
     * @return true if the client may enter read-only mode; false otherwise.
     */
    @JsonProperty
    public boolean isReadOnly() {
        return readOnly;
    }

    @JsonProperty
    public void isReadOnly(final boolean readOnly) {
        this.readOnly = readOnly;
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

    /**
     * Builds a default {@link ZooKeeper} instance..
     * <p/>
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
     * <p/>
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
     * <p/>
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
     * <p/>
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

        environment.admin().addHealthCheck(name, new ZooKeeperHealthCheck(client, namespace));
        environment.lifecycle().manage(new ManagedZooKeeper(client));

        return client;
    }
}
