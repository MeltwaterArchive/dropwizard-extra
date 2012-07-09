package com.datasift.dropwizard.hbase;

import com.datasift.dropwizard.hbase.config.HBaseClientConfiguration;
import com.datasift.dropwizard.hbase.scanner.BoundedRowScanner;
import com.datasift.dropwizard.hbase.scanner.RowScanner;
import com.datasift.dropwizard.hbase.util.*;
import com.stumbleupon.async.Deferred;
import com.yammer.dropwizard.util.Duration;
import com.yammer.dropwizard.util.Size;
import org.hbase.async.*;
import org.jboss.netty.util.Timer;

import java.util.ArrayList;
import java.util.concurrent.Semaphore;

/**
 * An {@link HBaseClient} that places an upper-bounds on the number of concurrent requests.
 *
 * This client places an upper-bounds on the number of concurrent requests awaiting
 * completion. When this limit is reached, subsequent requests will block until
 * existing requests have completed.
 *
 * This behaviour is particularly useful for throttling high-throughput bulk
 * insert applications where HBase is the bottle-neck. Without backing-off, such
 * an application may run out of memory. By setting the max requests to a
 * sufficiently high limit, but low enough so that it can be reached without
 * running out of memory, such applications can organically throttle and back-off
 * their writes.
 *
 * Book-keeping of in-flight requests is done using a {@link Semaphore} which is
 * configured as "non-fair" to reduce its impact on request throughput.
 */
public class BoundedHBaseClient implements HBaseClient {

    private HBaseClient client;
    private Semaphore semaphore;

    public static HBaseClient wrap(HBaseClientConfiguration configuration, HBaseClient client) {
        if (configuration.getMaxConcurrentRequests() > 0) {
            return new BoundedHBaseClient(client, configuration.getMaxConcurrentRequests());
        } else {
            return client;
        }
    }

    /**
     * Create a new instance with the specified maximum number of concurrent requests.
     *
     * @param client the underlying {@link HBaseClient} instance
     * @param maxRequests the maximum number of concurrent requests to allow
     */
    public BoundedHBaseClient(HBaseClient client, int maxRequests) {
        this.client = client;
        this.semaphore = new Semaphore(maxRequests);
    }

    public Duration getFlushInterval() {
        return client.getFlushInterval();
    }

    public Size getIncrementBufferSize() {
        return client.getIncrementBufferSize();
    }

    public Duration setFlushInterval(Duration flushInterval) {
        return client.setFlushInterval(flushInterval);
    }

    public Size setIncrementBufferSize(Size incrementBufferSize) {
        return client.setIncrementBufferSize(incrementBufferSize);
    }

    public Deferred<Boolean> create(PutRequest edit) {
        semaphore.acquireUninterruptibly();
        return client.create(edit).addBoth(new com.datasift.dropwizard.hbase.util.PermitReleasingCallback<Boolean>(semaphore));
    }

    public Deferred<Long> bufferIncrement(AtomicIncrementRequest request) {
        semaphore.acquireUninterruptibly();
        return client.bufferIncrement(request).addBoth(new com.datasift.dropwizard.hbase.util.PermitReleasingCallback<Long>(semaphore));
    }

    public Deferred<Long> increment(AtomicIncrementRequest request) {
        semaphore.acquireUninterruptibly();
        return client.increment(request).addBoth(new com.datasift.dropwizard.hbase.util.PermitReleasingCallback<Long>(semaphore));
    }

    public Deferred<Long> increment(AtomicIncrementRequest request, Boolean durable) {
        semaphore.acquireUninterruptibly();
        return client.increment(request, durable).addBoth(new com.datasift.dropwizard.hbase.util.PermitReleasingCallback<Long>(semaphore));
    }

    public Deferred<Boolean> compareAndSet(PutRequest edit, byte[] expected) {
        semaphore.acquireUninterruptibly();
        return client.compareAndSet(edit, expected).addBoth(new com.datasift.dropwizard.hbase.util.PermitReleasingCallback<Boolean>(semaphore));
    }

    public Deferred<Boolean> compareAndSet(PutRequest edit, String expected) {
        semaphore.acquireUninterruptibly();
        return client.compareAndSet(edit, expected).addBoth(new com.datasift.dropwizard.hbase.util.PermitReleasingCallback<Boolean>(semaphore));
    }

    public Deferred<Object> delete(DeleteRequest request) {
        semaphore.acquireUninterruptibly();
        return client.delete(request).addBoth(new com.datasift.dropwizard.hbase.util.PermitReleasingCallback<Object>(semaphore));
    }

    public Deferred<Object> ensureTableExists(byte[] table) {
        semaphore.acquireUninterruptibly();
        return client.ensureTableExists(table).addBoth(new com.datasift.dropwizard.hbase.util.PermitReleasingCallback<Object>(semaphore));
    }

    public Deferred<Object> ensureTableExists(String table) {
        semaphore.acquireUninterruptibly();
        return client.ensureTableExists(table).addBoth(new com.datasift.dropwizard.hbase.util.PermitReleasingCallback<Object>(semaphore));
    }

    public Deferred<Object> ensureTableFamilyExists(byte[] table, byte[] family) {
        semaphore.acquireUninterruptibly();
        return client.ensureTableFamilyExists(table, family).addBoth(new com.datasift.dropwizard.hbase.util.PermitReleasingCallback<Object>(semaphore));
    }

    public Deferred<Object> ensureTableFamilyExists(String table, String family) {
        semaphore.acquireUninterruptibly();
        return client.ensureTableFamilyExists(table, family).addBoth(new com.datasift.dropwizard.hbase.util.PermitReleasingCallback<Object>(semaphore));
    }

    public Deferred<Object> flush() {
        semaphore.acquireUninterruptibly();
        return client.flush().addBoth(new com.datasift.dropwizard.hbase.util.PermitReleasingCallback<Object>(semaphore));
    }

    public Deferred<ArrayList<KeyValue>> get(GetRequest request) {
        semaphore.acquireUninterruptibly();
        return client.get(request).addBoth(new com.datasift.dropwizard.hbase.util.PermitReleasingCallback<ArrayList<KeyValue>>(semaphore));
    }

    public Deferred<RowLock> lockRow(RowLockRequest request) {
        semaphore.acquireUninterruptibly();
        return client.lockRow(request).addBoth(new com.datasift.dropwizard.hbase.util.PermitReleasingCallback<RowLock>(semaphore));
    }

    public RowScanner newScanner(byte[] table) {
        return new BoundedRowScanner(client.newScanner(table), semaphore);
    }

    public RowScanner newScanner(String table) {
        return new BoundedRowScanner(client.newScanner(table), semaphore);
    }

    public Deferred<Object> put(PutRequest request) {
        semaphore.acquireUninterruptibly();
        return client.put(request).addBoth(new com.datasift.dropwizard.hbase.util.PermitReleasingCallback<Object>(semaphore));
    }

    public Deferred<Object> shutdown() {
        semaphore.acquireUninterruptibly();
        return client.shutdown().addBoth(new com.datasift.dropwizard.hbase.util.PermitReleasingCallback<Object>(semaphore));
    }

    public ClientStats stats() {
        return client.stats();
    }

    public Timer getTimer() {
        return client.getTimer();
    }

    public Deferred<Object> unlockRow(RowLock lock) {
        semaphore.acquireUninterruptibly();
        return client.unlockRow(lock).addBoth(new PermitReleasingCallback<Object>(semaphore));
    }
}
