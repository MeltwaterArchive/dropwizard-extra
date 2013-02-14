package com.datasift.dropwizard.hbase;

import com.datasift.dropwizard.hbase.metrics.HBaseInstrumentation;
import com.datasift.dropwizard.hbase.scanner.InstrumentedRowScanner;
import com.datasift.dropwizard.hbase.scanner.RowScanner;
import com.datasift.dropwizard.hbase.util.TimerStoppingCallback;
import com.stumbleupon.async.Deferred;
import com.yammer.dropwizard.util.Duration;
import com.yammer.dropwizard.util.Size;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.hbase.async.*;

import java.util.ArrayList;

/**
 * An {@link HBaseClient} that is instrumented with {@link Metric}s.
 * <p/>
 * For each asynchronous request method, a {@link Timer} tracks the time taken for the request.
 * <p/>
 * This implementation proxies all requests through an underlying {@link HBaseClient}, provided to
 * the {@link this#InstrumentedHBaseClient(HBaseClient) constructor}; it merely layers
 * instrumentation on top of the underlying {@link HBaseClient}.
 *
 * @see HBaseInstrumentation
 */
public class InstrumentedHBaseClient implements HBaseClient {

    /**
     * The underlying {@link HBaseClient} to dispatch requests.
     */
    private final HBaseClient client;

    /**
     * The instrumentation for this {@link HBaseClient}.
     */
    private final HBaseInstrumentation metrics;

    /**
     * Creates a new {@link InstrumentedHBaseClient} for the given underlying client.
     * <p/>
     * The {@link Metrics#defaultRegistry() default} {@link MetricsRegistry} will be used to
     * register the {@link Metric}s.
     *
     * @param client the underlying {@link HBaseClient} implementation to dispatch requests.
     */
    public InstrumentedHBaseClient(final HBaseClient client) {
        this(client, Metrics.defaultRegistry());
    }

    /**
     * Creates a new {@link InstrumentedHBaseClient} for the given underlying client.
     * <p/>
     * Instrumentation will be registered with the given {@link MetricsRegistry}.
     * <p/>
     * A new {@link HBaseInstrumentation} container will be created for this {@link HBaseClient}
     * with the given {@link MetricsRegistry}.
     *
     * @param client   the underlying {@link HBaseClient} implementation to dispatch requests.
     * @param registry the {@link MetricsRegistry} to register {@link Metric}s with.
     */
    public InstrumentedHBaseClient(final HBaseClient client, final MetricsRegistry registry) {
        this(client, new HBaseInstrumentation(client, registry));
    }

    /**
     * Creates a new {@link InstrumentedHBaseClient} for the given underlying client.
     * <p>
     * Instrumentation will be contained by the given {@link HBaseInstrumentation} instance.
     * <p>
     * <i>Note: this is only really useful for sharing instrumentation between multiple {@link
     * HBaseClient} instances, which only really makes sense for instances configured for the same
     * cluster, but with different client-side settings. <b>Use with caution!!</b></i>
     *
     * @param client  the underlying {@link HBaseClient} implementation to dispatch requests.
     * @param metrics the {@link HBaseInstrumentation} containing the {@link Metric}s to use.
     */
    public InstrumentedHBaseClient(final HBaseClient client, final HBaseInstrumentation metrics) {
        this.client = client;
        this.metrics = metrics;
    }

    /**
     * Get the maximum time for which edits may be buffered before being flushed.
     *
     * @return the maximum time for which edits may be buffered.
     *
     * @see HBaseClient#getFlushInterval()
     */
    public Duration getFlushInterval() {
        return client.getFlushInterval();
    }

    /**
     * Get the capacity of the increment buffer.
     *
     * @return the capacity of the increment buffer.
     *
     * @see HBaseClient#getIncrementBufferSize()
     */
    public Size getIncrementBufferSize() {
        return client.getIncrementBufferSize();
    }

    /**
     * Sets the maximum time for which edits may be buffered before being flushed.
     *
     * @param flushInterval the maximum time for which edits may be buffered.
     *
     * @return the previous flush interval.
     *
     * @see HBaseClient#setFlushInterval(Duration)
     */
    public Duration setFlushInterval(final Duration flushInterval) {
        return client.setFlushInterval(flushInterval);
    }

    /**
     * Sets the capacity of the increment buffer.
     *
     * @param incrementBufferSize the capacity of the increment buffer.
     *
     * @return the previous increment buffer capacity.
     *
     * @see HBaseClient#setIncrementBufferSize(Size)
     */
    public Size setIncrementBufferSize(final Size incrementBufferSize) {
        return client.setIncrementBufferSize(incrementBufferSize);
    }

    /**
     * Atomically creates a cell if, and only if, it doesn't already exist.
     *
     * @param edit the new cell to create.
     *
     * @return true if the cell was created, false if the cell already exists.
     *
     * @see HBaseClient#create(PutRequest)
     */
    public Deferred<Boolean> create(final PutRequest edit) {
        final TimerContext ctx = metrics.getCreates().time();
        return client.create(edit).addBoth(new TimerStoppingCallback<Boolean>(ctx));
    }

    /**
     * Buffer a durable increment for coalescing.
     *
     * @param request the increment to buffer.
     *
     * @return the new value of the cell, after the increment.
     *
     * @see HBaseClient#bufferIncrement(AtomicIncrementRequest) 
     */
    public Deferred<Long> bufferIncrement(final AtomicIncrementRequest request) {
        final TimerContext ctx = metrics.getIncrements().time();
        return client.bufferIncrement(request).addBoth(new TimerStoppingCallback<Long>(ctx));
    }

    /**
     * Atomically and durably increment a cell value.
     *
     * @param request the increment to make.
     *
     * @return the new value of the cell, after the increment.
     *
     * @see HBaseClient#increment(AtomicIncrementRequest)
     */
    public Deferred<Long> increment(final AtomicIncrementRequest request) {
        final TimerContext ctx = metrics.getIncrements().time();
        return client.increment(request).addBoth(new TimerStoppingCallback<Long>(ctx));
    }

    /**
     * Atomically increment a cell value, with optional durability.
     *
     * @param request the increment to make.
     * @param durable whether to guarantee this increment succeeded durably.
     *
     * @return the new value of the cell, after the increment.
     *
     * @see HBaseClient#increment(AtomicIncrementRequest, Boolean)
     */
    public Deferred<Long> increment(final AtomicIncrementRequest request, final Boolean durable) {
        final TimerContext ctx = metrics.getIncrements().time();
        return client.increment(request, durable).addBoth(new TimerStoppingCallback<Long>(ctx));
    }

    /**
     * Atomically compares and sets (CAS) a single cell
     *
     * @param edit the cell to set.
     * @param expected the expected current value.
     *
     * @return true if the expectation was met and the cell was set; otherwise, false.
     *
     * @see HBaseClient#compareAndSet(PutRequest, byte[])
     */
    public Deferred<Boolean> compareAndSet(final PutRequest edit, final byte[] expected) {
        final TimerContext ctx = metrics.getCompareAndSets().time();
        return client.compareAndSet(edit, expected)
                .addBoth(new TimerStoppingCallback<Boolean>(ctx));
    }

    /**
     * Atomically compares and sets (CAS) a single cell.
     *
     * @param edit the cell to set.
     * @param expected the expected current value.
     *
     * @return true if the expectation was met and the cell was set; otherwise, false.
     *
     * @see HBaseClient#compareAndSet(PutRequest, String)
     */
    public Deferred<Boolean> compareAndSet(final PutRequest edit, final String expected) {
        final TimerContext ctx = metrics.getCompareAndSets().time();
        return client.compareAndSet(edit, expected)
                .addBoth(new TimerStoppingCallback<Boolean>(ctx));
    }

    /**
     * Deletes the specified cells.
     *
     * @param request the cell(s) to delete.
     *
     * @return a {@link Deferred} indicating when the deletion completes.
     *
     * @see HBaseClient#delete(DeleteRequest)
     */
    public Deferred<Object> delete(final DeleteRequest request) {
        final TimerContext ctx = metrics.getDeletes().time();
        return client.delete(request).addBoth(new TimerStoppingCallback<Object>(ctx));
    }

    /**
     * Ensures that a specific table exists.
     *
     * @param table the table to check.
     *
     * @return a {@link Deferred} indicating the completion of the assertion.
     *
     * @throws TableNotFoundException (Deferred) if the table does not exist.
     *
     * @see HBaseClient#ensureTableExists(byte[])
     */
    public Deferred<Object> ensureTableExists(final byte[] table) {
        final TimerContext ctx = metrics.getAssertions().time();
        return client.ensureTableExists(table).addBoth(new TimerStoppingCallback<Object>(ctx));
    }

    /**
     * Ensures that a specific table exists.
     *
     * @param table the table to check.
     *
     * @return a {@link Deferred} indicating the completion of the assertion.
     *
     * @throws TableNotFoundException (Deferred) if the table does not exist.
     *
     * @see HBaseClient#ensureTableExists(String)
     */
    public Deferred<Object> ensureTableExists(final String table) {
        final TimerContext ctx = metrics.getAssertions().time();
        return client.ensureTableExists(table).addBoth(new TimerStoppingCallback<Object>(ctx));
    }

    /**
     * Ensures that a specific table exists.
     *
     * @param table the table to check.
     *
     * @return a {@link Deferred} indicating the completion of the assertion.
     *
     * @throws TableNotFoundException (Deferred) if the table does not exist.
     * @throws NoSuchColumnFamilyException (Deferred) if the family doesn't exist.
     *
     * @see HBaseClient#ensureTableFamilyExists(byte[], byte[])
     */
    public Deferred<Object> ensureTableFamilyExists(final byte[] table, final byte[] family) {
        final TimerContext ctx = metrics.getAssertions().time();
        return client.ensureTableFamilyExists(table, family)
                .addBoth(new TimerStoppingCallback<Object>(ctx));
    }

    /**
     * Ensures that a specific table exists.
     *
     * @param table the table to check.
     *
     * @return a {@link Deferred} indicating the completion of the assertion.
     *
     * @throws TableNotFoundException (Deferred) if the table does not exist.
     * @throws NoSuchColumnFamilyException (Deferred) if the family doesn't exist.
     *
     * @see HBaseClient#ensureTableFamilyExists(String, String)
     */
    public Deferred<Object> ensureTableFamilyExists(final String table, final String family) {
        final TimerContext ctx = metrics.getAssertions().time();
        return client.ensureTableFamilyExists(table, family)
                .addBoth(new TimerStoppingCallback<Object>(ctx));
    }

    /**
     * Flushes all requests buffered on the client-side.
     *
     * @return a {@link Deferred} indicating the completion of the flush.
     *
     * @see HBaseClient#flush()
     */
    public Deferred<Object> flush() {
        final TimerContext ctx = metrics.getFlushes().time();
        return client.flush().addBoth(new TimerStoppingCallback<Object>(ctx));
    }

    /**
     * Retrieves the specified cells.
     *
     * @param request the cells to get.
     *
     * @return the requested cells.
     *
     * @see HBaseClient#get(GetRequest)
     */
    public Deferred<ArrayList<KeyValue>> get(final GetRequest request) {
        final TimerContext ctx = metrics.getGets().time();
        return client.get(request).addBoth(new TimerStoppingCallback<ArrayList<KeyValue>>(ctx));
    }

    /**
     * Aqcuire an explicit row lock.
     *
     * @param request the row(s) to lock.
     *
     * @return the row lock.
     *
     * @see HBaseClient#lockRow(RowLockRequest)
     */
    public Deferred<RowLock> lockRow(final RowLockRequest request) {
        final TimerContext ctx = metrics.getLocks().time();
        return client.lockRow(request).addBoth(new TimerStoppingCallback<RowLock>(ctx));
    }

    /**
     * Create a new {@link RowScanner} for a table.
     *
     * @param table the table to scan.
     *
     * @return a new {@link RowScanner} for the specified table.
     *
     * @see HBaseClient#scan(byte[])
     */
    public RowScanner scan(final byte[] table) {
        return new InstrumentedRowScanner(client.scan(table), metrics);
    }

    /**
     * Create a new {@link RowScanner} for a table.
     *
     * @param table the table to scan.
     *
     * @return a new {@link RowScanner} for the specified table.
     *
     * @see HBaseClient#scan(String)
     */
    public RowScanner scan(final String table) {
        return new InstrumentedRowScanner(client.scan(table), metrics);
    }

    /**
     * Store the specified cell(s).
     *
     * @param request the cell(s) to store.
     *
     * @return a {@link Deferred} indicating the completion of the put operation.
     *
     * @see HBaseClient#put(PutRequest)
     */
    public Deferred<Object> put(final PutRequest request) {
        final TimerContext ctx = metrics.getPuts().time();
        return client.put(request).addBoth(new TimerStoppingCallback<Object>(ctx));
    }

    /**
     * Performs a graceful shutdown of this client, flushing any pending requests.
     *
     * @return a {@link Deferred} indicating the completion of the shutdown operation.
     *
     * @see HBaseClient#shutdown()
     */
    public Deferred<Object> shutdown() {
        return client.shutdown();
    }

    /**
     * Get an immutable snapshot of client usage statistics.
     *
     * @return an immutable snapshot of client usage statistics.
     *
     * @see HBaseClient#stats()
     */
    public ClientStats stats() {
        return client.stats();
    }

    /**
     * Get the underlying {@link org.jboss.netty.util.Timer} used by the client.
     *
     * @return the underlying {@link org.jboss.netty.util.Timer} used by the async client.
     *
     * @see HBaseClient#getTimer()
     */
    public org.jboss.netty.util.Timer getTimer() {
        return client.getTimer();
    }

    /**
     * Release an explicit row lock.
     *
     * @param lock the lock to release.
     *
     * @return a {@link Deferred} indicating the completion of the unlock operation.
     *
     * @see HBaseClient#unlockRow(RowLock)
     */
    public Deferred<Object> unlockRow(final RowLock lock) {
        final TimerContext ctx = metrics.getUnlocks().time();
        return client.unlockRow(lock).addBoth(new TimerStoppingCallback<Object>(ctx));
    }
}
