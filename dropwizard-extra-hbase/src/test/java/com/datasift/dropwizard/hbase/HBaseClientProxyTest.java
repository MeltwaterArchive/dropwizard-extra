package com.datasift.dropwizard.hbase;

import com.stumbleupon.async.Deferred;
import io.dropwizard.util.Duration;
import io.dropwizard.util.Size;
import org.hbase.async.*;
import org.jboss.netty.util.Timer;
import org.junit.Test;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;

import static org.mockito.Mockito.*;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests {@link HBaseClientProxy}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({
        org.hbase.async.HBaseClient.class,
        PutRequest.class,
        GetRequest.class,
        AtomicIncrementRequest.class,
        DeleteRequest.class,
        RowLockRequest.class,
        RowLock.class,
        ClientStats.class
})
public class HBaseClientProxyTest {

    private HBaseClient client;
    private org.hbase.async.HBaseClient underlying;

    @Before
    public void setup() {
        underlying = mock(org.hbase.async.HBaseClient.class);
        client = new HBaseClientProxy(underlying);
    }

    @Test
    public void proxiesFlushInterval() {
        when(underlying.getFlushInterval()).thenReturn((short) 5000);

        assertThat("flush interval is proxied and boxed",
                client.getFlushInterval(), is(Duration.milliseconds(5000)));
    }

    @Test
    public void proxiesIncrementBufferSize() {
        when(underlying.getIncrementBufferSize()).thenReturn(10240);

        assertThat("increment buffer size is proxied and boxed",
                client.getIncrementBufferSize(), is(Size.bytes(10240)));
    }

    @Test
    public void setsFlushInterval() {
        when(underlying.setFlushInterval((short) 10000)).thenReturn((short) 5000);

        assertThat("flush interval begins with default",
                client.setFlushInterval(Duration.milliseconds(10000)),
                is(Duration.milliseconds(5000)));

        when(underlying.getFlushInterval()).thenReturn((short) 10000);

        assertThat("setting flush interval is unboxed and proxied",
                client.getFlushInterval(), is(Duration.milliseconds(10000)));
    }

    @Test
    public void setsIncrementBufferSize() {
        when(underlying.setIncrementBufferSize(2097152)).thenReturn(10240);

        assertThat("increment buffer size began with default",
                client.setIncrementBufferSize(Size.megabytes(2)),
                is(Size.bytes(10240)));

        when(underlying.getIncrementBufferSize()).thenReturn(2097152);
        assertThat("setting increment buffer size is unboxed and proxied",
                client.getIncrementBufferSize(), is(Size.bytes(2097152)));
    }

    @Test
    public void createsCell() {
        final PutRequest req = mock(PutRequest.class);
        final Deferred<Boolean> resp = new Deferred<>();
        when(underlying.atomicCreate(req)).thenReturn(resp);

        assertThat("creates cell via proxy", client.create(req), is(resp));
    }

    @Test
    public void buffersIncrement() {
        final AtomicIncrementRequest req = mock(AtomicIncrementRequest.class);
        final Deferred<Long> resp = new Deferred<>();
        when(underlying.bufferAtomicIncrement(req)).thenReturn(resp);

        assertThat("buffers increment for a cell via proxy",
                client.bufferIncrement(req), is(resp));
    }

    @Test
    public void increments() {
        final AtomicIncrementRequest req = mock(AtomicIncrementRequest.class);
        final Deferred<Long> resp = new Deferred<>();
        when(underlying.atomicIncrement(req)).thenReturn(resp);

        assertThat("increments cell via proxy",
                client.increment(req), is(resp));
    }

    @Test
    public void incrementsDurably() {
        final AtomicIncrementRequest req = mock(AtomicIncrementRequest.class);
        final Deferred<Long> resp = new Deferred<>();
        when(underlying.atomicIncrement(req, true)).thenReturn(resp);

        assertThat("durably increments cell via proxy",
                client.increment(req, true), is(resp));
    }

    @Test
    public void comparesAndSets() {
        final PutRequest req = mock(PutRequest.class);
        final byte[] expected = new byte[] { 0x0 };
        final Deferred<Boolean> resp = new Deferred<>();
        when(underlying.compareAndSet(req, expected)).thenReturn(resp);
        when(underlying.compareAndSet(req, new String(expected))).thenReturn(resp);

        assertThat("compares and sets bytes cell via proxy",
                client.compareAndSet(req, expected), is(resp));

        assertThat("compares and sets String cell via proxy",
                client.compareAndSet(req, new String(expected)), is(resp));
    }

    @Test
    public void deletes() {
        final DeleteRequest req = mock(DeleteRequest.class);
        final Deferred<Object> resp = new Deferred<>();
        when(underlying.delete(req)).thenReturn(resp);

        assertThat("deletes cell via proxy", client.delete(req), is(resp));
    }

    @Test
    public void ensuresTableExists() {
        final String table = "table";
        final Deferred<Object> resp = new Deferred<>();
        when(underlying.ensureTableExists(table)).thenReturn(resp);
        when(underlying.ensureTableExists(table.getBytes())).thenReturn(resp);

        assertThat("ensures table String exists via proxy",
                client.ensureTableExists(table), is(resp));
        assertThat("ensures table bytes exists via proxy",
                client.ensureTableExists(table.getBytes()), is(resp));
    }

    @Test
    public void ensuresTableAndFamilyExist() {
        final String table = "table";
        final String family = "family";
        final Deferred<Object> resp = new Deferred<>();
        when(underlying.ensureTableFamilyExists(table, family)).thenReturn(resp);
        when(underlying.ensureTableFamilyExists(table.getBytes(), family.getBytes()))
                .thenReturn(resp);

        assertThat("ensures table and family Strings exist via proxy",
                client.ensureTableFamilyExists(table, family), is(resp));
        assertThat("ensures table and family bytes exist via proxy",
                client.ensureTableFamilyExists(table.getBytes(), family.getBytes()),
                is(resp));
    }

    @Test
    public void flushes() {
        final Deferred<Object> resp = new Deferred<>();
        when(underlying.flush()).thenReturn(resp);

        assertThat("flushes via proxy", client.flush(), is(resp));
    }

    @Test
    public void gets() {
        final GetRequest req = mock(GetRequest.class);
        final Deferred<ArrayList<KeyValue>> resp = new Deferred<>();
        when(underlying.get(req)).thenReturn(resp);

        assertThat("gets cell(s) via proxy", client.get(req), is(resp));
    }

    @Test
    public void locksRow() {
        final RowLockRequest req = mock(RowLockRequest.class);
        final Deferred<RowLock> resp = new Deferred<>();
        when(underlying.lockRow(req)).thenReturn(resp);

        assertThat("locks row via proxy", client.lockRow(req), is(resp));
    }

    @Test
    public void puts() {
        final PutRequest req = mock(PutRequest.class);
        final Deferred<Object> resp = new Deferred<>();
        when(underlying.put(req)).thenReturn(resp);

        assertThat("puts row(s) via proxy", client.put(req), is(resp));
    }

    @Test
    public void shutsdown() {
        final Deferred<Object> resp = new Deferred<>();
        when(underlying.shutdown()).thenReturn(resp);

        assertThat("shutsdown via proxy", client.shutdown(), is(resp));
    }

    @Test
    public void clientStats() {
        final ClientStats stats = mock(ClientStats.class);
        when(underlying.stats()).thenReturn(stats);

        assertThat("gets client stats via proxy", client.stats(), is(stats));
    }

    @Test
    public void timer() {
        final Timer timer = mock(Timer.class);
        when(underlying.getTimer()).thenReturn(timer);

        assertThat("gets underlying timer via proxy",
                client.getTimer(), is(timer));
    }

    @Test
    public void unlocksRow() {
        final RowLock lock = mock(RowLock.class);
        final Deferred<Object> resp = new Deferred<>();
        when(underlying.unlockRow(lock)).thenReturn(resp);

        assertThat("unlocks row(s) via proxy", client.unlockRow(lock), is(resp));
    }
}
