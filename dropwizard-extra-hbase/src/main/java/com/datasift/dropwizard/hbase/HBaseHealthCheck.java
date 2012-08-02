package com.datasift.dropwizard.hbase;

import com.stumbleupon.async.TimeoutException;
import com.yammer.metrics.core.HealthCheck;
import org.hbase.async.TableNotFoundException;

/**
 * A {@link HealthCheck} for an HBase table using an {@link HBaseClient}.
 */
public class HBaseHealthCheck extends HealthCheck {

    private final HBaseClient client;
    private final String table;

    public HBaseHealthCheck(final HBaseClient client,
                            final String name,
                            final String table) {
        super(name + " (HBase): " + table);

        this.client = client;
        this.table = table;
    }

    @Override
    protected Result check() throws Exception {
        try {
            client.ensureTableExists(table.getBytes()).joinUninterruptibly(5000);
            return Result.healthy();
        } catch (final TimeoutException e) {
            return Result.unhealthy(String.format(
                    "Timed out checking for '%s' after 5 seconds", table));
        } catch (final TableNotFoundException e) {
            return Result.unhealthy(String.format(
                    "Table '%s' does not exist", table));
        }
    }
}
