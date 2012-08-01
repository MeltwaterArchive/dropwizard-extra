package com.datasift.dropwizard.hbase;

import com.stumbleupon.async.Deferred;
import org.hbase.async.*;
import org.junit.Test;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.concurrent.Semaphore;

import static org.mockito.Mockito.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/**
 * Tests {@link HBaseClientProxy}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({
        Deferred.class,
        PutRequest.class,
        GetRequest.class,
        AtomicIncrementRequest.class,
        DeleteRequest.class,
        RowLockRequest.class,
        RowLock.class,
        ClientStats.class
})
public class BoundedHBaseClientTest {

    private Semaphore semaphore;
    private HBaseClient underlying;
    private HBaseClient client;

    @Before
    public void setup() {
        semaphore = new Semaphore(1);
        underlying = mock(HBaseClient.class);
        client = new BoundedHBaseClient(underlying, semaphore);
    }

    @Test
    public void createsWithPermit() {
        PutRequest req = mock(PutRequest.class);
        Deferred<Boolean> resp = new Deferred<Boolean>();

        when(underlying.create(req)).thenReturn(resp);

        checkForPermit();
        assertThat("creates cell(s) without blocking",
                client.create(req), is(resp));
        checkCallbackReleasesPermit(resp);
    }

    @Test(expected = BlockedException.class)
    public void createBlocksWithNoPermit() {
        checkBlocksWithNoPermitOn().create(mock(PutRequest.class));
    }

    @Test
    public void buffersIncrementWithPermit() {
        AtomicIncrementRequest req = mock(AtomicIncrementRequest.class);
        Deferred<Long> resp = new Deferred<Long>();

        when(underlying.bufferIncrement(req)).thenReturn(resp);

        checkForPermit();
        assertThat("buffers increment without blocking",
                client.bufferIncrement(req), is(resp));
        checkCallbackReleasesPermit(resp);
    }

    @Test(expected = BlockedException.class)
    public void bufferedIncrementBlocksWithNoPermit() {
        checkBlocksWithNoPermitOn().bufferIncrement(mock(AtomicIncrementRequest.class));
    }

    @Test
    public void incrementWithPermit() {
        AtomicIncrementRequest req = mock(AtomicIncrementRequest.class);
        Deferred<Long> resp = new Deferred<Long>();

        when(underlying.increment(req)).thenReturn(resp);

        checkForPermit();
        assertThat("increments without blocking",
                client.increment(req), is(resp));
        checkCallbackReleasesPermit(resp);
    }

    @Test(expected = BlockedException.class)
    public void incrementBlocksWithNoPermit() {
        checkBlocksWithNoPermitOn().increment(mock(AtomicIncrementRequest.class));
    }

    @Test
    public void durablyIncrementWithPermit() {
        AtomicIncrementRequest req = mock(AtomicIncrementRequest.class);
        Deferred<Long> resp = new Deferred<Long>();

        when(underlying.increment(req, true)).thenReturn(resp);

        checkForPermit();
        assertThat("increments durably without blocking",
                client.increment(req, true), is(resp));
        checkCallbackReleasesPermit(resp);
    }

    @Test(expected = BlockedException.class)
    public void durablyIncrementBlocksWithNoPermit() {
        checkBlocksWithNoPermitOn()
                .increment(mock(AtomicIncrementRequest.class), true);
    }

    @Test
    public void comparesAndSetsWithPermit() {
        PutRequest req = mock(PutRequest.class);
        Deferred<Boolean> resp = new Deferred<Boolean>();

        when(underlying.compareAndSet(req, new byte[0])).thenReturn(resp);

        checkForPermit();
        assertThat("increments durably without blocking",
                client.compareAndSet(req, new byte[0]), is(resp));
        checkCallbackReleasesPermit(resp);
    }

    @Test(expected = BlockedException.class)
    public void compareAndSetBlocksWithNoPermit() {
        checkBlocksWithNoPermitOn()
                .compareAndSet(mock(PutRequest.class), new byte[0]);
    }

    @Test
    public void comparesAndSetsStringsWithPermit() {
        PutRequest req = mock(PutRequest.class);
        Deferred<Boolean> resp = new Deferred<Boolean>();

        when(underlying.compareAndSet(req, "")).thenReturn(resp);

        checkForPermit();
        assertThat("compares and sets Strings without blocking",
                client.compareAndSet(req, ""), is(resp));
        checkCallbackReleasesPermit(resp);
    }

    @Test(expected = BlockedException.class)
    public void compareAndSetStringBlocksWithNoPermit() {
        checkBlocksWithNoPermitOn()
                .compareAndSet(mock(PutRequest.class), "");
    }

    @Test
    public void deletesWithPermit() {
        DeleteRequest req = mock(DeleteRequest.class);
        Deferred<Object> resp = new Deferred<Object>();

        when(underlying.delete(req)).thenReturn(resp);

        checkForPermit();
        assertThat("deletes without blocking", client.delete(req), is(resp));
        checkCallbackReleasesPermit(resp);
    }

    @Test(expected = BlockedException.class)
    public void deleteBlocksWithNoPermit() {
        checkBlocksWithNoPermitOn().delete(mock(DeleteRequest.class));
    }


    @Test
    public void ensuresTableExistsWithPermit() {
        Deferred<Object> resp = new Deferred<Object>();

        when(underlying.ensureTableExists(new byte[0])).thenReturn(resp);

        checkForPermit();
        assertThat("ensuring a table exists without blocking",
                client.ensureTableExists(new byte[0]), is(resp));
        checkCallbackReleasesPermit(resp);
    }

    @Test(expected = BlockedException.class)
    public void ensureTableExistBlocksWithNoPermit() {
        checkBlocksWithNoPermitOn().ensureTableExists(new byte[0]);
    }

    @Test
    public void ensuresStringTableExistsWithPermit() {
        Deferred<Object> resp = new Deferred<Object>();

        when(underlying.ensureTableExists("")).thenReturn(resp);

        checkForPermit();
        assertThat("ensuring a table String exists without blocking",
                client.ensureTableExists(""), is(resp));
        checkCallbackReleasesPermit(resp);
    }

    @Test(expected = BlockedException.class)
    public void ensureStringTableExistBlocksWithNoPermit() {
        checkBlocksWithNoPermitOn().ensureTableExists("");
    }


    @Test
    public void ensuresTableAndFamilyExistsWithPermit() {
        Deferred<Object> resp = new Deferred<Object>();

        when(underlying.ensureTableFamilyExists(new byte[0], new byte[0]))
                .thenReturn(resp);

        checkForPermit();
        assertThat("ensuring table and family exist without blocking",
                client.ensureTableFamilyExists(new byte[0], new byte[0]),
                is(resp));
        checkCallbackReleasesPermit(resp);
    }

    @Test(expected = BlockedException.class)
    public void ensureTableAndFamilyExistBlocksWithNoPermit() {
        checkBlocksWithNoPermitOn()
                .ensureTableFamilyExists(new byte[0], new byte[0]);
    }

    @Test
    public void ensuresStringTableAndFamilyExistsWithPermit() {
        Deferred<Object> resp = new Deferred<Object>();

        when(underlying.ensureTableFamilyExists("", "")).thenReturn(resp);

        checkForPermit();
        assertThat("ensuring table and family Strings exist without blocking",
                client.ensureTableFamilyExists("", ""), is(resp));
        checkCallbackReleasesPermit(resp);
    }

    @Test(expected = BlockedException.class)
    public void ensureStringTableAndFamilyExistBlocksWithNoPermit() {
        checkBlocksWithNoPermitOn().ensureTableFamilyExists("", "");
    }

    @Test
    public void flushesWithPermit() {
        Deferred<Object> resp = new Deferred<Object>();

        when(underlying.flush()).thenReturn(resp);

        checkForPermit();
        assertThat("flushes without blocking", client.flush(), is(resp));
        checkCallbackReleasesPermit(resp);
    }

    @Test(expected = BlockedException.class)
    public void flushBlocksWithNoPermit() {
        checkBlocksWithNoPermitOn().flush();
    }

    @Test
    public void getsWithPermit() {
        GetRequest req = mock(GetRequest.class);
        Deferred<ArrayList<KeyValue>> resp =
                new Deferred<ArrayList<KeyValue>>();

        when(underlying.get(req)).thenReturn(resp);

        checkForPermit();
        assertThat("gets without blocking", client.get(req), is(resp));
        checkCallbackReleasesPermit(resp);
    }

    @Test(expected = BlockedException.class)
    public void getBlocksWithNoPermit() {
        checkBlocksWithNoPermitOn().get(mock(GetRequest.class));
    }

    @Test
    public void locksWithPermit() {
        RowLockRequest req = mock(RowLockRequest.class);
        Deferred<RowLock> resp = new Deferred<RowLock>();

        when(underlying.lockRow(req)).thenReturn(resp);

        checkForPermit();
        assertThat("locks row without blocking", client.lockRow(req), is(resp));
        checkCallbackReleasesPermit(resp);
    }

    @Test(expected = BlockedException.class)
    public void lockBlocksWithNoPermit() {
        checkBlocksWithNoPermitOn().lockRow(mock(RowLockRequest.class));
    }

    @Test
    public void putWithPermit() {
        PutRequest req = mock(PutRequest.class);
        Deferred<Object> resp = new Deferred<Object>();

        when(underlying.put(req)).thenReturn(resp);

        checkForPermit();
        assertThat("puts without blocking", client.put(req), is(resp));
        checkCallbackReleasesPermit(resp);
    }

    @Test(expected = BlockedException.class)
    public void putBlocksWithNoPermit() {
        checkBlocksWithNoPermitOn().put(mock(PutRequest.class));
    }

    @Test
    public void unlocksWithPermit() {
        RowLock req = mock(RowLock.class);
        Deferred<Object> resp = new Deferred<Object>();

        when(underlying.unlockRow(req)).thenReturn(resp);

        checkForPermit();
        assertThat("unlocks row without blocking",
                client.unlockRow(req), is(resp));
        checkCallbackReleasesPermit(resp);
    }

    @Test(expected = BlockedException.class)
    public void unlockBlocksWithNoPermit() {
        checkBlocksWithNoPermitOn().unlockRow(mock(RowLock.class));
    }

    private HBaseClient checkBlocksWithNoPermitOn() {
        Semaphore semaphore = mock(Semaphore.class);
        doThrow(new BlockedException()).when(semaphore).acquireUninterruptibly();
        return new BoundedHBaseClient(underlying, semaphore);
    }

    private <T> void checkCallbackReleasesPermit(Deferred<T> callback) {
        checkNoPermits();
        callback.callback(null);
        checkForPermit();
    }

    private void checkForPermit() {
        assertThat("semaphore has a permit",
                semaphore.availablePermits(), is(1));
    }

    private void checkNoPermits() {
        assertThat("semaphore has no more permits",
                semaphore.availablePermits(), is(0));
    }

    class BlockedException extends RuntimeException {
        public BlockedException() {
            super("Blocked on semaphore");
        }
    }
}
