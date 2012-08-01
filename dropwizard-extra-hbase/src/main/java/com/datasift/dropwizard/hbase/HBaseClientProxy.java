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

    private org.hbase.async.HBaseClient client;

    public HBaseClientProxy(org.hbase.async.HBaseClient client) {
        this.client = client;
    }

    public Duration getFlushInterval() {
        return Duration.milliseconds(client.getFlushInterval());
    }

    public Size getIncrementBufferSize() {
        return Size.bytes(client.getIncrementBufferSize());
    }

    public Duration setFlushInterval(Duration flushInterval) {
        final short interval = (short) flushInterval.toMilliseconds();
        return Duration.milliseconds(client.setFlushInterval(interval));
    }

    public Size setIncrementBufferSize(Size incrementBufferSize) {
        final int size = (int) incrementBufferSize.toBytes();
        return Size.bytes(client.setIncrementBufferSize(size));
    }

    public Deferred<Boolean> create(PutRequest edit) {
        return client.atomicCreate(edit);
    }

    public Deferred<Long> bufferIncrement(AtomicIncrementRequest request) {
        return client.bufferAtomicIncrement(request);
    }

    public Deferred<Long> increment(AtomicIncrementRequest request) {
        return client.atomicIncrement(request);
    }

    public Deferred<Long> increment(AtomicIncrementRequest request,
                                    Boolean durable) {
        return client.atomicIncrement(request, durable);
    }

    public Deferred<Boolean> compareAndSet(PutRequest edit, byte[] expected) {
        return client.compareAndSet(edit, expected);
    }

    public Deferred<Boolean> compareAndSet(PutRequest edit, String expected) {
        return client.compareAndSet(edit, expected);
    }

    public Deferred<Object> delete(DeleteRequest request) {
        return client.delete(request);
    }

    public Deferred<Object> ensureTableExists(byte[] table) {
        return client.ensureTableExists(table);
    }

    public Deferred<Object> ensureTableExists(String table) {
        return client.ensureTableExists(table);
    }

    public Deferred<Object> ensureTableFamilyExists(byte[] table,
                                                    byte[] family) {
        return client.ensureTableFamilyExists(table, family);
    }

    public Deferred<Object> ensureTableFamilyExists(String table,
                                                    String family) {
        return client.ensureTableFamilyExists(table, family);
    }

    public Deferred<Object> flush() {
        return client.flush();
    }

    public Deferred<ArrayList<KeyValue>> get(GetRequest request) {
        return client.get(request);
    }

    public Deferred<RowLock> lockRow(RowLockRequest request) {
        return client.lockRow(request);
    }

    public RowScanner newScanner(byte[] table) {
        return new RowScannerProxy(client.newScanner(table));
    }

    public RowScanner newScanner(String table) {
        return new RowScannerProxy(client.newScanner(table));
    }

    public Deferred<Object> put(PutRequest request) {
        return client.put(request);
    }

    public Deferred<Object> shutdown() {
        return client.shutdown();
    }

    public ClientStats stats() {
        return client.stats();
    }

    public Timer getTimer() {
        return client.getTimer();
    }

    public Deferred<Object> unlockRow(RowLock lock) {
        return client.unlockRow(lock);
    }
}
