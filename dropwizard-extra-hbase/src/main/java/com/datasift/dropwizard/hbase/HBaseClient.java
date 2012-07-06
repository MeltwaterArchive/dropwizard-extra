package com.datasift.dropwizard.hbase;

import com.datasift.dropwizard.hbase.scanner.RowScanner;
import com.stumbleupon.async.Deferred;
import com.yammer.dropwizard.util.Duration;
import com.yammer.dropwizard.util.Size;
import org.hbase.async.*;
import org.jboss.netty.util.Timer;

import java.util.ArrayList;

/**
 * Client for interacting with an HBase cluster.
 *
 * To create an instance, use {@link HBaseClientFactory}.
 *
 * All implementations are wrapper proxies around {@link org.hbase.async.HBaseClient}
 * providing additional functionality.
 *
 * @see HBaseClientFactory
 * @see org.hbase.async.HBaseClient
 */
public interface HBaseClient {

    /**
     * Get the maximum time for which edits may be buffered before being flushed.
     *
     * @see org.hbase.async.HBaseClient#getFlushInterval()
     * @return the maximum time for which edits may be buffered
     */
    public Duration getFlushInterval();

    /**
     * Get the capacity of the increment buffer.
     *
     * @see org.hbase.async.HBaseClient#getIncrementBufferSize()
     * @return the capacity of the increment buffer
     */
    public Size getIncrementBufferSize();

    /**
     * Sets the maximum time for which edits may be buffered before being flushed.
     *
     * @see org.hbase.async.HBaseClient#setFlushInterval(short)
     * @param flushInterval the maximum time for which edits may be buffered
     * @return the previous flush interval
     */
    public Duration setFlushInterval(Duration flushInterval);

    /**
     * Sets the capacity of the increment buffer.
     *
     * @see org.hbase.async.HBaseClient#setIncrementBufferSize(int)
     * @param incrementBufferSize the capacity of the increment buffer
     * @return the previous increment buffer capacity
     */
    public Size setIncrementBufferSize(Size incrementBufferSize);

    /**
     * Atomically creates a cell if, and only if, it doesn't already exist.
     *
     * @see org.hbase.async.HBaseClient#atomicCreate(org.hbase.async.PutRequest)
     * @param edit the new cell to create
     * @return true if the cell was created, false if the cell already exists
     */
    public Deferred<Boolean> create(PutRequest edit);

    /**
     * Buffer a durable increment for coalescing.
     *
     * @see org.hbase.async.HBaseClient#bufferAtomicIncrement(org.hbase.async.AtomicIncrementRequest)
     * @param request the increment to buffer
     * @return the new value of the cell, after the increment
     */
    public Deferred<Long> bufferIncrement(AtomicIncrementRequest request);

    /**
     * Atomically and durably increment a cell value.
     *
     * @see org.hbase.async.HBaseClient#atomicIncrement(org.hbase.async.AtomicIncrementRequest)
     * @param request the increment to make
     * @return the new value of the cell, after the increment
     */
    public Deferred<Long> increment(AtomicIncrementRequest request);

    /**
     * Atomically increment a cell value, with optional durability.
     *
     * @see org.hbase.async.HBaseClient#atomicIncrement(org.hbase.async.AtomicIncrementRequest, boolean)
     * @param request the increment to make
     * @param durable whether to guarantee this increment succeeded durably
     * @return the new value of the cell, after the increment
     */
    public Deferred<Long> increment(AtomicIncrementRequest request, Boolean durable);

    /**
     * Atomically compares and sets (CAS) a single cell
     *
     * @see org.hbase.async.HBaseClient#compareAndSet(org.hbase.async.PutRequest, byte[])
     * @param edit the cell to set
     * @param expected the expected current value
     * @return true if the expectation was met and the cell was set, otherwise, false
     */
    public Deferred<Boolean> compareAndSet(PutRequest edit, byte[] expected);

    /**
     * Atomically compares and sets (CAS) a single cell.
     *
     * @see org.hbase.async.HBaseClient#compareAndSet(org.hbase.async.PutRequest, String)
     * @param edit the cell to set
     * @param expected the expected current value
     * @return true if the expectation was met and the cell was set, otherwise, false
     */
    public Deferred<Boolean> compareAndSet(PutRequest edit, String expected);

    /**
     * Deletes the specified cells
     *
     * @see org.hbase.async.HBaseClient#delete(org.hbase.async.DeleteRequest)
     * @param request the cell(s) to delete
     * @return a {@link Deferred} indicating when the deletion completes
     */
    public Deferred<Object> delete(DeleteRequest request);

    /**
     * Ensures that a specific table exists.
     *
     * @see org.hbase.async.HBaseClient#ensureTableExists(byte[])
     * @param table the table to check
     * @throws TableNotFoundException (Deferred) if the table does not exist
     * @return a {@link Deferred} indicating the completion of the assertion
     */
    public Deferred<Object> ensureTableExists(byte[] table);

    /**
     * Ensures that a specific table exists.
     *
     * @see org.hbase.async.HBaseClient#ensureTableExists(String)
     * @param table the table to check
     * @throws TableNotFoundException (Deferred) if the table does not exist
     * @return a {@link Deferred} indicating the completion of the assertion
     */
    public Deferred<Object> ensureTableExists(String table);

    /**
     * Ensures that a specific table exists.
     *
     * @see org.hbase.async.HBaseClient#ensureTableFamilyExists(byte[], byte[])
     * @param table the table to check
     * @throws TableNotFoundException (Deferred) if the table does not exist
     * @throws NoSuchColumnFamilyException (Deferred) if the family doesn't exist
     * @return a {@link Deferred} indicating the completion of the assertion
     */
    public Deferred<Object> ensureTableFamilyExists(byte[] table, byte[] family);

    /**
     * Ensures that a specific table exists.
     *
     * @see org.hbase.async.HBaseClient#ensureTableFamilyExists(String, String)
     * @param table the table to check
     * @throws TableNotFoundException (Deferred) if the table does not exist
     * @throws NoSuchColumnFamilyException (Deferred) if the family doesn't exist
     * @return a {@link Deferred} indicating the completion of the assertion
     */
    public Deferred<Object> ensureTableFamilyExists(String table, String family);

    /**
     * Flushes all requests buffered on the client-side
     *
     * @see org.hbase.async.HBaseClient#flush()
     * @return a {@link Deferred} indicating the completion of the flush
     */
    public Deferred<Object> flush();

    /**
     * Retrieves the specified cells
     *
     * @see org.hbase.async.HBaseClient#get(org.hbase.async.GetRequest)
     * @param request the cells to get
     * @return the requested cells
     */
    public Deferred<ArrayList<KeyValue>> get(GetRequest request);

    /**
     * Aqcuire an explicit row lock.
     *
     * @see org.hbase.async.HBaseClient#lockRow(org.hbase.async.RowLockRequest)
     * @param request the row(s) to lock
     * @return the row lock
     */
    public Deferred<RowLock> lockRow(RowLockRequest request);

    /**
     * Create a new {@link RowScanner} for a table.
     *
     * @see org.hbase.async.HBaseClient#newScanner(byte[])
     * @param table the table to scan
     * @return a new {@link RowScanner} for the specified table
     */
    public RowScanner newScanner(byte[] table);

    /**
     * Create a new {@link RowScanner} for a table.
     *
     * @see org.hbase.async.HBaseClient#newScanner(String)
     * @param table the table to scan
     * @return a new {@link RowScanner} for the specified table
     */
    public RowScanner newScanner(String table);

    /**
     * Store the specified cell(s).
     *
     * @see org.hbase.async.HBaseClient#put(org.hbase.async.PutRequest)
     * @param request the cell(s) to store
     * @return a {@link Deferred} indicating the completion of the store operation
     */
    public Deferred<Object> put(PutRequest request);

    /**
     * Performs a graceful shutdown of this client, flushing any pending requests.
     *
     * @see org.hbase.async.HBaseClient#shutdown()
     * @return a {@link Deferred} indicating the completion of the shutdown operation
     */
    public Deferred<Object> shutdown();

    /**
     * Get an immutable snapshot of client usage statistics.
     *
     * @see org.hbase.async.HBaseClient#stats()
     * @return an immutable snapshot of client usage statistics
     */
    public ClientStats stats();

    /**
     * Get the underlying {@link Timer} used by the async client
     *
     * @see org.hbase.async.HBaseClient#getTimer()
     * @return the underlying {@link Timer} used by the async client
     */
    public Timer getTimer();

    /**
     * Release an explicit row lock.
     *
     * @see org.hbase.async.HBaseClient#unlockRow(org.hbase.async.RowLock)
     * @param lock the lock to release
     * @return a {@link Deferred} indicating the completion of the unlock operation
     */
    public Deferred<Object> unlockRow(RowLock lock);
}
