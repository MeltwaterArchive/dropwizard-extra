package com.datasift.dropwizard.hbase;

import com.stumbleupon.async.TimeoutException;
import com.yammer.metrics.core.HealthCheck;
import org.hbase.async.TableNotFoundException;

/**
 * A {@link HealthCheck} for an HBase table using an {@link HBaseClient}.
 */
public class HBaseHealthCheck extends HealthCheck {

    private HBaseClient client;
    private String table;

    public HBaseHealthCheck(HBaseClient client, String name, String table) {
        super(name + "-hbase: " + table);

        this.client = client;
        this.table = table;
    }

    @Override
    protected Result check() throws Exception {
        try {
            client.ensureTableExists(table.getBytes()).joinUninterruptibly(5000);
            return Result.healthy();
        } catch (TimeoutException e) {
            return Result.unhealthy("Timed out checking for '" + table + "' after 5 seconds");
        } catch (TableNotFoundException e) {
            return Result.unhealthy("Table '" + table + "' does not exist");
        }
    }
}
