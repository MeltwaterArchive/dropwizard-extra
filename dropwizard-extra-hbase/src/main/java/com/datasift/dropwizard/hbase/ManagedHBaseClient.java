package com.datasift.dropwizard.hbase;

import io.dropwizard.lifecycle.Managed;
import io.dropwizard.util.Duration;

/**
 * Manages the lifecycle of an {@link HBaseClient}.
 */
public class ManagedHBaseClient implements Managed {

    private final HBaseClient client;
    private final Duration connectionTimeout;

    /**
     * Manage the specified {@link HBaseClient} with the given {@code connectionTimeout}.
     *
     * @param client the {@link HBaseClient} to manage.
     * @param connectionTimeout the maximum time to wait for a connection to a region server or
     *                          ZooKeeper quorum.
     */
    public ManagedHBaseClient(final HBaseClient client, final Duration connectionTimeout) {
        this.client = client;
        this.connectionTimeout = connectionTimeout;
    }

    /**
     * Forces connection of the {@link HBaseClient}.
     *
     * To force the connection, we look for the prescence of the .META. table.
     *
     * @throws com.stumbleupon.async.TimeoutException if there is a problem connecting to HBase.
     * @throws org.hbase.async.TableNotFoundException if the .META. table can't be found.
     * @throws Exception if there is a problem verifying the .META. table exists.
     */
    public void start() throws Exception {
        client.ensureTableExists(".META.").joinUninterruptibly(connectionTimeout.toMilliseconds());
    }

    /**
     * Shutsdown the {@link HBaseClient}, waiting until shutdown is complete.
     *
     * @throws Exception if there is a problem shutting the {@link HBaseClient} down.
     */
    public void stop() throws Exception {
        client.shutdown().joinUninterruptibly();
    }
}
