package com.datasift.dropwizard.hbase;

import com.stumbleupon.async.TimeoutException;
import com.codahale.metrics.health.HealthCheck;
import org.hbase.async.TableNotFoundException;

/**
 * A {@link HealthCheck} for an HBase table using an {@link HBaseClient}.
 */
public class HBaseHealthCheck extends HealthCheck {

    private final HBaseClient client;
    private final String table;

    /**
     * Checks the health of the given {@link HBaseClient} by connecting and testing for the given
     * {@code table}.
     *
     * @param client the client to check the health of.
     * @param table the name of the table to look for.
     */
    public HBaseHealthCheck(final HBaseClient client, final String table) {
        this.client = client;
        this.table = table;
    }

    /**
     * Checks the health of the configured {@link HBaseClient} by using it to test for the
     * configured {@code table}.
     *
     * @return {@link Result#healthy()} if the client can be used to confirm the table exists; or
     *         {@link Result#unhealthy(String)} either if the table does not exist or the client
     *         times out while checking for the table.
     *
     * @throws Exception if an unexpected Exception occurs while checking the health of the client.
     */
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
