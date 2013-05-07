package com.datasift.dropwizard.hbase;

import com.datasift.dropwizard.hbase.scanner.RowScanner;
import com.stumbleupon.async.Deferred;
import com.codahale.dropwizard.util.Duration;
import com.codahale.dropwizard.util.Size;
import org.hbase.async.*;
import org.jboss.netty.util.Timer;

import java.util.ArrayList;

/**
 * Client for interacting with an HBase cluster.
 * <p/>
 * To create an instance, use {@link HBaseClientFactory}.
 * <p/>
 * All implementations are wrapper proxies around {@link org.hbase.async.HBaseClient} providing
 * additional functionality.
 *
 * @see HBaseClientFactory
 * @see org.hbase.async.HBaseClient
 */
public interface HBaseClient {

    /**
     * Get the maximum time for which edits may be buffered before being flushed.
     *
     * @return the maximum time for which edits may be buffered.
     *
     * @see org.hbase.async.HBaseClient#getFlushInterval()
     */
    public Duration getFlushInterval();

    /**
     * Get the capacity of the increment buffer.
     *
     * @return the capacity of the increment buffer.
     *
     * @see org.hbase.async.HBaseClient#getIncrementBufferSize()
     */
    public Size getIncrementBufferSize();

    /**
     * Sets the maximum time for which edits may be buffered before being flushed.
     *
     * @param flushInterval the maximum time for which edits may be buffered.
     *
     * @return the previous flush interval.
     *
     * @see org.hbase.async.HBaseClient#setFlushInterval(short)
     */
    public Duration setFlushInterval(Duration flushInterval);

    /**
     * Sets the capacity of the increment buffer.
     *
     * @param incrementBufferSize the capacity of the increment buffer.
     *
     * @return the previous increment buffer capacity.
     *
     * @see org.hbase.async.HBaseClient#setIncrementBufferSize(int)
     */
    public Size setIncrementBufferSize(Size incrementBufferSize);

    /**
     * Atomically creates a cell if, and only if, it doesn't already exist.
     *
     * @param edit the new cell to create.
     *
     * @return true if the cell was created, false if the cell already exists.
     *
     * @see org.hbase.async.HBaseClient#atomicCreate(org.hbase.async.PutRequest)
     */
    public Deferred<Boolean> create(PutRequest edit);

    /**
     * Buffer a durable increment for coalescing.
     *
     * @param request the increment to buffer
     *
     * @return the new value of the cell, after the increment.
     *
     * @see org.hbase.async.HBaseClient#bufferAtomicIncrement(org.hbase.async.AtomicIncrementRequest)
     */
    public Deferred<Long> bufferIncrement(AtomicIncrementRequest request);

    /**
     * Atomically and durably increment a cell value.
     *
     * @param request the increment to make.
     *
     * @return the new value of the cell, after the increment.
     *
     * @see org.hbase.async.HBaseClient#atomicIncrement(org.hbase.async.AtomicIncrementRequest)
     */
    public Deferred<Long> increment(AtomicIncrementRequest request);

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
    public Deferred<Long> increment(AtomicIncrementRequest request, Boolean durable);

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
    public Deferred<Boolean> compareAndSet(PutRequest edit, byte[] expected);

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
    public Deferred<Boolean> compareAndSet(PutRequest edit, String expected);

    /**
     * Deletes the specified cells
     *
     * @param request the cell(s) to delete.
     *
     * @return a {@link Deferred} indicating when the deletion completes.
     *
     * @see org.hbase.async.HBaseClient#delete(org.hbase.async.DeleteRequest)
     */
    public Deferred<Object> delete(DeleteRequest request);

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
    public Deferred<Object> ensureTableExists(byte[] table);

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
    public Deferred<Object> ensureTableExists(String table);

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
    public Deferred<Object> ensureTableFamilyExists(byte[] table, byte[] family);

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
    public Deferred<Object> ensureTableFamilyExists(String table, String family);

    /**
     * Flushes all requests buffered on the client-side
     *
     * @return a {@link Deferred} indicating the completion of the flush.
     *
     * @see org.hbase.async.HBaseClient#flush()
     */
    public Deferred<Object> flush();

    /**
     * Retrieves the specified cells
     *
     * @param request the cells to get.
     *
     * @return the requested cells.
     *
     * @see org.hbase.async.HBaseClient#get(org.hbase.async.GetRequest)
     */
    public Deferred<ArrayList<KeyValue>> get(GetRequest request);

    /**
     * Aqcuire an explicit row lock.
     *
     * @param request the row(s) to lock.
     *
     * @return the row lock.
     *
     * @see org.hbase.async.HBaseClient#lockRow(org.hbase.async.RowLockRequest)
     */
    public Deferred<RowLock> lockRow(RowLockRequest request);

    /**
     * Create a new {@link RowScanner} for a table.
     *
     * @param table the table to scan.
     *
     * @return a new {@link RowScanner} for the specified table.
     *
     * @see org.hbase.async.HBaseClient#newScanner(byte[])
     */
    public RowScanner scan(byte[] table);

    /**
     * Create a new {@link RowScanner} for a table.
     *
     * @param table the table to scan.
     *
     * @return a new {@link RowScanner} for the specified table.
     *
     * @see org.hbase.async.HBaseClient#newScanner(String)
     */
    public RowScanner scan(String table);

    /**
     * Store the specified cell(s).
     *
     * @param request the cell(s) to store.
     *
     * @return a {@link Deferred} indicating the completion of the store operation.
     *
     * @see org.hbase.async.HBaseClient#put(org.hbase.async.PutRequest)
     */
    public Deferred<Object> put(PutRequest request);

    /**
     * Performs a graceful shutdown of this client, flushing any pending requests.
     *
     * @return a {@link Deferred} indicating the completion of the shutdown operation.
     *
     * @see org.hbase.async.HBaseClient#shutdown()
     */
    public Deferred<Object> shutdown();

    /**
     * Get an immutable snapshot of client usage statistics.
     *
     * @return an immutable snapshot of client usage statistics.
     *
     * @see org.hbase.async.HBaseClient#stats()
     */
    public ClientStats stats();

    /**
     * Get the underlying {@link Timer} used by the async client
     *
     * @return the underlying {@link Timer} used by the async client.
     *
     * @see org.hbase.async.HBaseClient#getTimer()
     */
    public Timer getTimer();

    /**
     * Release an explicit row lock.
     *
     * @param lock the lock to release.
     *
     * @return a {@link Deferred} indicating the completion of the unlock operation.
     * 
     * @see org.hbase.async.HBaseClient#unlockRow(org.hbase.async.RowLock)
     */
    public Deferred<Object> unlockRow(RowLock lock);
}
