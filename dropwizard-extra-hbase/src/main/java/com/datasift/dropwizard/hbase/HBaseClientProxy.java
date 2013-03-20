package com.datasift.dropwizard.hbase;

import com.datasift.dropwizard.hbase.scanner.RowScanner;
import com.datasift.dropwizard.hbase.scanner.RowScannerProxy;
import com.stumbleupon.async.Deferred;
import com.yammer.dropwizard.util.Duration;
import com.yammer.dropwizard.util.Size;
import org.hbase.async.*;
import org.jboss.netty.util.Timer;

import java.util.ArrayList;

/**
 * Proxies the {@link HBaseClient} API to an {@link org.hbase.async.HBaseClient}.
 */
public class HBaseClientProxy implements HBaseClient {

    private final org.hbase.async.HBaseClient client;

    /**
     * Initialises this proxy for the given underlying {@code client}.
     *
     * @param client the client to proxy requests to.
     */
    public HBaseClientProxy(final org.hbase.async.HBaseClient client) {
        this.client = client;
    }

    /**
     * Get the maximum time for which edits may be buffered before being flushed.
     *
     * @return the maximum time for which edits may be buffered.
     *
     * @see org.hbase.async.HBaseClient#getFlushInterval()
     */
    public Duration getFlushInterval() {
        return Duration.milliseconds(client.getFlushInterval());
    }

    /**
     * Get the capacity of the increment buffer.
     *
     * @return the capacity of the increment buffer.
     *
     * @see org.hbase.async.HBaseClient#getIncrementBufferSize()
     */
    public Size getIncrementBufferSize() {
        return Size.bytes(client.getIncrementBufferSize());
    }

    /**
     * Sets the maximum time for which edits may be buffered before being flushed.
     *
     * @param flushInterval the maximum time for which edits may be buffered.
     *
     * @return the previous flush interval.
     *
     * @see org.hbase.async.HBaseClient#setFlushInterval(short)
     */
    public Duration setFlushInterval(final Duration flushInterval) {
        final short interval = (short) flushInterval.toMilliseconds();
        return Duration.milliseconds(client.setFlushInterval(interval));
    }

    /**
     * Sets the capacity of the increment buffer.
     *
     * @param incrementBufferSize the capacity of the increment buffer.
     *
     * @return the previous increment buffer capacity.
     *
     * @see org.hbase.async.HBaseClient#setIncrementBufferSize(int)
     */
    public Size setIncrementBufferSize(final Size incrementBufferSize) {
        final int size = (int) incrementBufferSize.toBytes();
        return Size.bytes(client.setIncrementBufferSize(size));
    }

    /**
     * Atomically creates a cell if, and only if, it doesn't already exist.
     *
     * @param edit the new cell to create.
     *
     * @return true if the cell was created, false if the cell already exists.
     *
     * @see org.hbase.async.HBaseClient#atomicCreate(org.hbase.async.PutRequest)
     */
    public Deferred<Boolean> create(final PutRequest edit) {
        return client.atomicCreate(edit);
    }

    /**
     * Buffer a durable increment for coalescing.
     *
     * @param request the increment to buffer
     *
     * @return the new value of the cell, after the increment.
     *
     * @see org.hbase.async.HBaseClient#bufferAtomicIncrement(org.hbase.async.AtomicIncrementRequest)
     */
    public Deferred<Long> bufferIncrement(final AtomicIncrementRequest request) {
        return client.bufferAtomicIncrement(request);
    }

    /**
     * Atomically and durably increment a cell value.
     *
     * @param request the increment to make.
     *
     * @return the new value of the cell, after the increment.
     *
     * @see org.hbase.async.HBaseClient#atomicIncrement(org.hbase.async.AtomicIncrementRequest)
     */
    public Deferred<Long> increment(final AtomicIncrementRequest request) {
        return client.atomicIncrement(request);
    }

    /**
     * Atomically increment a cell value, with optional durability.
     *
     * @param request the increment to make.
     * @param durable whether to guarantee this increment succeeded durably.
     *
     * @return the new value of the cell, after the increment.
     *
     * @see org.hbase.async.HBaseClient#atomicIncrement(org.hbase.async.AtomicIncrementRequest, boolean)
     */
    public Deferred<Long> increment(final AtomicIncrementRequest request,
                                    final Boolean durable) {
        return client.atomicIncrement(request, durable);
    }

    /**
     * Atomically compares and sets (CAS) a single cell
     *
     * @param edit the cell to set.
     * @param expected the expected current value.
     *
     * @return true if the expectation was met and the cell was set, otherwise, false.
     *
     * @see org.hbase.async.HBaseClient#compareAndSet(org.hbase.async.PutRequest, byte[])
     */
    public Deferred<Boolean> compareAndSet(final PutRequest edit,
                                           final byte[] expected) {
        return client.compareAndSet(edit, expected);
    }

    /**
     * Atomically compares and sets (CAS) a single cell.
     *
     * @param edit the cell to set.
     * @param expected the expected current value.
     *
     * @return true if the expectation was met and the cell was set, otherwise, false.
     *
     * @see org.hbase.async.HBaseClient#compareAndSet(org.hbase.async.PutRequest, String)
     */
    public Deferred<Boolean> compareAndSet(final PutRequest edit,
                                           final String expected) {
        return client.compareAndSet(edit, expected);
    }

    /**
     * Deletes the specified cells
     *
     * @param request the cell(s) to delete.
     *
     * @return a {@link Deferred} indicating when the deletion completes.
     *
     * @see org.hbase.async.HBaseClient#delete(org.hbase.async.DeleteRequest)
     */
    public Deferred<Object> delete(final DeleteRequest request) {
        return client.delete(request);
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
     * @see org.hbase.async.HBaseClient#ensureTableExists(byte[])
     */
    public Deferred<Object> ensureTableExists(final byte[] table) {
        return client.ensureTableExists(table);
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
     * @see org.hbase.async.HBaseClient#ensureTableExists(String)
     */
    public Deferred<Object> ensureTableExists(final String table) {
        return client.ensureTableExists(table);
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
     * @see org.hbase.async.HBaseClient#ensureTableFamilyExists(byte[], byte[])
     */
    public Deferred<Object> ensureTableFamilyExists(final byte[] table,
                                                    final byte[] family) {
        return client.ensureTableFamilyExists(table, family);
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
     * @see org.hbase.async.HBaseClient#ensureTableFamilyExists(String, String)
     */
    public Deferred<Object> ensureTableFamilyExists(final String table,
                                                    final String family) {
        return client.ensureTableFamilyExists(table, family);
    }

    /**
     * Flushes all requests buffered on the client-side
     *
     * @return a {@link Deferred} indicating the completion of the flush.
     *
     * @see org.hbase.async.HBaseClient#flush()
     */
    public Deferred<Object> flush() {
        return client.flush();
    }

    /**
     * Retrieves the specified cells
     *
     * @param request the cells to get.
     *
     * @return the requested cells.
     *
     * @see org.hbase.async.HBaseClient#get(org.hbase.async.GetRequest)
     */
    public Deferred<ArrayList<KeyValue>> get(final GetRequest request) {
        return client.get(request);
    }

    /**
     * Aqcuire an explicit row lock.
     *
     * @param request the row(s) to lock.
     *
     * @return the row lock.
     *
     * @see org.hbase.async.HBaseClient#lockRow(org.hbase.async.RowLockRequest)
     */
    public Deferred<RowLock> lockRow(final RowLockRequest request) {
        return client.lockRow(request);
    }

    /**
     * Create a new {@link RowScanner} for a table.
     *
     * @param table the table to scan.
     *
     * @return a new {@link RowScanner} for the specified table.
     *
     * @see org.hbase.async.HBaseClient#newScanner(byte[])
     */
    public RowScanner scan(final byte[] table) {
        return new RowScannerProxy(client.newScanner(table));
    }

    /**
     * Create a new {@link RowScanner} for a table.
     *
     * @param table the table to scan.
     *
     * @return a new {@link RowScanner} for the specified table.
     *
     * @see org.hbase.async.HBaseClient#newScanner(String)
     */
    public RowScanner scan(final String table) {
        return new RowScannerProxy(client.newScanner(table));
    }

    /**
     * Store the specified cell(s).
     *
     * @param request the cell(s) to store.
     *
     * @return a {@link Deferred} indicating the completion of the store operation.
     *
     * @see org.hbase.async.HBaseClient#put(org.hbase.async.PutRequest)
     */
    public Deferred<Object> put(final PutRequest request) {
        return client.put(request);
    }

    /**
     * Performs a graceful shutdown of this client, flushing any pending requests.
     *
     * @return a {@link Deferred} indicating the completion of the shutdown operation.
     *
     * @see org.hbase.async.HBaseClient#shutdown()
     */
    public Deferred<Object> shutdown() {
        return client.shutdown();
    }

    /**
     * Get an immutable snapshot of client usage statistics.
     *
     * @return an immutable snapshot of client usage statistics.
     *
     * @see org.hbase.async.HBaseClient#stats()
     */
    public ClientStats stats() {
        return client.stats();
    }

    /**
     * Get the underlying {@link Timer} used by the async client
     *
     * @return the underlying {@link Timer} used by the async client.
     *
     * @see org.hbase.async.HBaseClient#getTimer()
     */
    public Timer getTimer() {
        return client.getTimer();
    }

    /**
     * Release an explicit row lock.
     *
     * @param lock the lock to release.
     *
     * @return a {@link Deferred} indicating the completion of the unlock operation.
     *
     * @see org.hbase.async.HBaseClient#unlockRow(org.hbase.async.RowLock)
     */
    public Deferred<Object> unlockRow(final RowLock lock) {
        return client.unlockRow(lock);
    }
}
