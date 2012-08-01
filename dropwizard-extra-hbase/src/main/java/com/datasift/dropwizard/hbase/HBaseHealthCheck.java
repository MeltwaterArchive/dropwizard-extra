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
        super(name + " (HBase): " + table);

        this.client = client;
        this.table = table;
    }

    @Override
    protected Result check() throws Exception {
        try {
            client.ensureTableExists(table.getBytes()).joinUninterruptibly(5000);
            return Result.healthy();
        } catch (TimeoutException e) {
            return Result.unhealthy(String.format(
                    "Timed out checking for '%s' after 5 seconds", table));
        } catch (TableNotFoundException e) {
            return Result.unhealthy(String.format(
                    "Table '%s' does not exist", table));
        }
    }
}
