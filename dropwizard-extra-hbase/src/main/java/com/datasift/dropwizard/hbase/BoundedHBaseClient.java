package com.datasift.dropwizard.hbase;

import com.datasift.dropwizard.hbase.scanner.BoundedRowScanner;
import com.datasift.dropwizard.hbase.scanner.RowScanner;
import com.datasift.dropwizard.hbase.util.*;
import com.stumbleupon.async.Deferred;
import io.dropwizard.util.Duration;
import io.dropwizard.util.Size;
import org.hbase.async.*;
import org.jboss.netty.util.Timer;

import java.util.ArrayList;
import java.util.concurrent.Semaphore;

/**
 * An {@link HBaseClient} that constrains the maximum number of concurrent asynchronous requests.
 * <p/>
 * This client places an upper-bounds on the number of concurrent asynchronous requests awaiting
 * completion. When this limit is reached, subsequent requests will block until an existing request
 * completes.
 * <p/>
 * This behaviour is particularly useful for throttling high-throughput applications where HBase is
 * the bottle-neck. Without backing-off, such an application may run out of memory. By constraining
 * the maximum number of requests to a sufficiently high limit, but low enough so that it can be
 * reached without running out of memory, such applications can organically throttle and back-off
 * their requests.
 * <p/>
 * Book-keeping of in-flight requests is done using a {@link Semaphore} which is configured as
 * "non-fair" to reduce its impact on request throughput.
 */
public class BoundedHBaseClient implements HBaseClient {

    /**
     * The underlying {@link HBaseClient} to dispatch requests.
     */
    private final HBaseClient client;

    /**
     * The {@link Semaphore} constraining the maximum number of concurrent
     * asynchronous requests.
     */
    private final Semaphore semaphore;

    /**
     * Create a new instance with the given limit on concurrent requests for the given underlying
     * {@link HBaseClient} implementation.
     *
     * @param client the underlying {@link HBaseClient} implementation
     * @param maxRequests the maximum number of concurrent requests
     */
    public BoundedHBaseClient(final HBaseClient client, final int maxRequests) {
        this(client, new Semaphore(maxRequests));
    }

    /**
     * Create a new instance with the given semaphore for the given underlying {@link HBaseClient}
     * implementation.
     * <p/>
     * <i>Note: this is only really useful for sharing a {@link Semaphore} between two {@link
     * BoundedHBaseClient} instances, which only really makes sense for instances configured for
     * the same cluster, but with different client-side settings. <b>Use with caution!!</b></i>
     *
     * @param client the underlying {@link HBaseClient} implementation.
     * @param semaphore the {@link Semaphore} to track concurrent asynchronous requests with.
     */
    public BoundedHBaseClient(final HBaseClient client, final Semaphore semaphore) {
        this.client = client;
        this.semaphore = semaphore;
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
        semaphore.acquireUninterruptibly();
        return client.create(edit)
                .addBoth(new PermitReleasingCallback<Boolean>(semaphore));
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
        semaphore.acquireUninterruptibly();
        return client.bufferIncrement(request)
                .addBoth(new PermitReleasingCallback<Long>(semaphore));
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
        semaphore.acquireUninterruptibly();
        return client.increment(request).addBoth(new PermitReleasingCallback<Long>(semaphore));
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
    public Deferred<Long> increment(final AtomicIncrementRequest request,
                                    final Boolean durable) {
        semaphore.acquireUninterruptibly();
        return client.increment(request, durable)
                .addBoth(new PermitReleasingCallback<Long>(semaphore));
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
    public Deferred<Boolean> compareAndSet(final PutRequest edit,
                                           final byte[] expected) {
        semaphore.acquireUninterruptibly();
        return client.compareAndSet(edit, expected)
                .addBoth(new PermitReleasingCallback<Boolean>(semaphore));
    }

    /**
     * Atomically compares and sets (CAS) a single cell.
     *
     * @param edit     the cell to set.
     * @param expected the expected current value.
     *
     * @return true if the expectation was met and the cell was set; otherwise, false.
     *
     * @see HBaseClient#compareAndSet(PutRequest, String)
     */
    public Deferred<Boolean> compareAndSet(final PutRequest edit,
                                           final String expected) {
        semaphore.acquireUninterruptibly();
        return client.compareAndSet(edit, expected)
                .addBoth(new PermitReleasingCallback<Boolean>(semaphore));
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
        semaphore.acquireUninterruptibly();
        return client.delete(request).addBoth(new PermitReleasingCallback<>(semaphore));
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
        semaphore.acquireUninterruptibly();
        return client.ensureTableExists(table)
                .addBoth(new PermitReleasingCallback<>(semaphore));
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
        semaphore.acquireUninterruptibly();
        return client.ensureTableExists(table)
                .addBoth(new PermitReleasingCallback<>(semaphore));
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
    public Deferred<Object> ensureTableFamilyExists(final byte[] table,
                                                    final byte[] family) {
        semaphore.acquireUninterruptibly();
        return client.ensureTableFamilyExists(table, family)
                .addBoth(new PermitReleasingCallback<>(semaphore));
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
    public Deferred<Object> ensureTableFamilyExists(final String table,
                                                    final String family) {
        semaphore.acquireUninterruptibly();
        return client.ensureTableFamilyExists(table, family)
                .addBoth(new PermitReleasingCallback<>(semaphore));
    }

    /**
     * Flushes all requests buffered on the client-side
     *
     * @return a {@link Deferred} indicating the completion of the flush.
     *
     * @see HBaseClient#flush()
     */
    public Deferred<Object> flush() {
        semaphore.acquireUninterruptibly();
        return client.flush().addBoth(new PermitReleasingCallback<>(semaphore));
    }

    /**
     * Retrieves the specified cells
     *
     * @param request the cells to get.
     *
     * @return the requested cells.
     *
     * @see HBaseClient#get(GetRequest)
     */
    public Deferred<ArrayList<KeyValue>> get(final GetRequest request) {
        semaphore.acquireUninterruptibly();
        return client.get(request)
                .addBoth(new PermitReleasingCallback<ArrayList<KeyValue>>(semaphore));
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
        semaphore.acquireUninterruptibly();
        return client.lockRow(request).addBoth(new PermitReleasingCallback<RowLock>(semaphore));
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
        return new BoundedRowScanner(client.scan(table), semaphore);
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
        return new BoundedRowScanner(client.scan(table), semaphore);
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
        semaphore.acquireUninterruptibly();
        return client.put(request).addBoth(new PermitReleasingCallback<>(semaphore));
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
     * @see HBaseClient#unlockRow(RowLock)
     */
    public Deferred<Object> unlockRow(final RowLock lock) {
        semaphore.acquireUninterruptibly();
        return client.unlockRow(lock).addBoth(new PermitReleasingCallback<>(semaphore));
    }
}
