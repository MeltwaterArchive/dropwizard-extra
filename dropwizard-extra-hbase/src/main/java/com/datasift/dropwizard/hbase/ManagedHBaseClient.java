package com.datasift.dropwizard.hbase;

import com.datasift.dropwizard.hbase.config.HBaseClientConfiguration;
import com.yammer.dropwizard.lifecycle.Managed;

/**
 * Manages the lifecycle of an {@link HBaseClient}.
 */
public class ManagedHBaseClient implements Managed {

    private HBaseClient client;
    private HBaseClientConfiguration configuration;

    /**
     * Manage the specified {@link HBaseClient} with the given {@link HBaseClientConfiguration}
     * @param client the {@link HBaseClient} to manage
     * @param configuration a {@link HBaseClientConfiguration} to use for the connection timeout
     */
    public ManagedHBaseClient(HBaseClient client, HBaseClientConfiguration configuration) {
        this.client = client;
        this.configuration = configuration;
    }

    /**
     * Forces connection of the {@link HBaseClient}.
     *
     * To force the connection, we look for the prescence of the .META. table.
     *
     * @throws com.stumbleupon.async.TimeoutException if there is a problem connecting to HBase
     * @throws org.hbase.async.TableNotFoundException if the .META. table can't be found
     * @throws Exception if there is a problem verifying the .META. table exists
     */
    public void start() throws Exception {
        client.ensureTableExists(".META.")
                .joinUninterruptibly(configuration.getConnectionTimeout().toMilliseconds());
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
