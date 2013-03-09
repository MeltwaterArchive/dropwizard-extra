package com.datasift.dropwizard.hbase;

import com.datasift.dropwizard.hbase.metrics.HBaseInstrumentation;
import com.stumbleupon.async.Deferred;
import com.yammer.dropwizard.util.Duration;
import com.yammer.dropwizard.util.Size;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.reporting.JmxReporter;
import org.hbase.async.*;
import org.junit.Test;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;

import static org.hamcrest.Matchers.closeTo;
import static org.mockito.Mockito.*;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests {@link InstrumentedHBaseClient}.
 * <p/>
 * Each method is tested first, that it proxies its implementation to the underlying {@link
 * HBaseClient}, and then that the method is timed as expected.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({
        PutRequest.class,
        GetRequest.class,
        AtomicIncrementRequest.class,
        DeleteRequest.class,
        RowLockRequest.class,
        RowLock.class,
        ClientStats.class
})
public class InstrumentedHBaseClientTest {

    static {
        // undo damage done by JmxReporter (part 1)
        // JmxReporter adds itself to the default MetricsRegistry in a static
        // initializer and doesn't play nice with multiple MetricsRegistry's
        // so we forcibly remove it here
        Metrics.defaultRegistry().removeListener(JmxReporter.getDefault());
    }

    private HBaseClient underlying;
    private HBaseInstrumentation metrics;
    private final MetricsRegistry registry = new MetricsRegistry(new Clock() {

        private long val = 0;

        @Override
        public long tick() {
            return val += 50000000;
        }
    });

    @Before
    public void setup() {
        underlying = mock(HBaseClient.class);
        metrics = mock(HBaseInstrumentation.class);

        // undo damage done by JmxReporter (part 2, see above)+
        registry.removeListener(JmxReporter.getDefault());
    }

    @Test
    public void getsFlushInterval() {
        when(underlying.getFlushInterval()).thenReturn(Duration.seconds(10));

        assertThat("gets flush interval via proxy",
                new InstrumentedHBaseClient(underlying, "test")
                        .getFlushInterval(),
                is(Duration.seconds(10)));
    }

    @Test
    public void getsIncrementBufferSize() {
        when(underlying.getIncrementBufferSize()).thenReturn(Size.bytes(1024));

        assertThat("gets increment buffer size via proxy",
                new InstrumentedHBaseClient(underlying, "test")
                        .getIncrementBufferSize(),
                is(Size.bytes(1024)));
    }

    @Test
    public void setsFlushInterval() {
        when(underlying.setFlushInterval(Duration.seconds(5)))
                .thenReturn(Duration.seconds(10));
        assertThat("sets flush interval via proxy",
                new InstrumentedHBaseClient(underlying, "test")
                        .setFlushInterval(Duration.seconds(5)),
                is(Duration.seconds(10)));
    }

    @Test
    public void setsIncrementBufferSize() {
        when(underlying.setIncrementBufferSize(Size.bytes(2048)))
                .thenReturn(Size.bytes(1024));
        assertThat("sets increment buffer size via proxy",
                new InstrumentedHBaseClient(underlying, "test")
                        .setIncrementBufferSize(Size.bytes(2048)),
                is(Size.bytes(1024)));
    }

    @Test
    public void proxiesCreates() {
        final PutRequest req = mock(PutRequest.class);
        final Deferred<Boolean> resp = new Deferred<Boolean>();
        when(underlying.create(req)).thenReturn(resp);

        assertThat("creates cell(s) via proxy",
                new InstrumentedHBaseClient(underlying, "test").create(req), is(resp));
    }

    @Test
    public void timesCreates() {
        final PutRequest req = mock(PutRequest.class);
        final Deferred<Boolean> resp = new Deferred<Boolean>();
        final Timer timer = registry.newTimer(
                underlying.getClass(), "create", "requests");

        when(underlying.create(req)).thenReturn(resp);
        when(metrics.getCreates()).thenReturn(timer);

        new InstrumentedHBaseClient(underlying, metrics)
                .create(req).callback(new Object());

        assertThat("times creation of cell(s)",
                timer.max(), is(closeTo(50.0, 0.001)));
    }

    @Test
    public void proxiesBufferedIncrements() {
        final AtomicIncrementRequest req = mock(AtomicIncrementRequest.class);
        final Deferred<Long> resp = new Deferred<Long>();
        when(underlying.bufferIncrement(req)).thenReturn(resp);

        assertThat("buffers increment(s) via proxy",
                new InstrumentedHBaseClient(underlying, "test").bufferIncrement(req),
                is(resp));
    }

    @Test
    public void timesBufferedIncrements() {
        final AtomicIncrementRequest req = mock(AtomicIncrementRequest.class);
        final Deferred<Long> resp = new Deferred<Long>();
        final Timer timer = registry.newTimer(
                underlying.getClass(), "increment", "requests");

        when(underlying.bufferIncrement(req)).thenReturn(resp);
        when(metrics.getIncrements()).thenReturn(timer);

        new InstrumentedHBaseClient(underlying, metrics)
                .bufferIncrement(req).callback(new Object());

        assertThat("times buffered increment(s)",
                timer.max(), is(closeTo(50.0, 0.001)));
    }

    @Test
    public void proxiesIncrements() {
        final AtomicIncrementRequest req = mock(AtomicIncrementRequest.class);
        final Deferred<Long> resp = new Deferred<Long>();
        when(underlying.increment(req)).thenReturn(resp);

        assertThat("increment(s) via proxy",
                new InstrumentedHBaseClient(underlying, "test").increment(req),
                is(resp));
    }

    @Test
    public void timesIncrements() {
        final AtomicIncrementRequest req = mock(AtomicIncrementRequest.class);
        final Deferred<Long> resp = new Deferred<Long>();
        final Timer timer = registry.newTimer(
                underlying.getClass(), "increment", "requests");

        when(underlying.increment(req)).thenReturn(resp);
        when(metrics.getIncrements()).thenReturn(timer);

        new InstrumentedHBaseClient(underlying, metrics)
                .increment(req).callback(new Object());

        assertThat("times increment(s)",
                timer.max(), is(closeTo(50.0, 0.001)));
    }


    @Test
    public void proxiesDurableIncrements() {
        final AtomicIncrementRequest req = mock(AtomicIncrementRequest.class);
        final Deferred<Long> resp = new Deferred<Long>();
        when(underlying.increment(req, true)).thenReturn(resp);

        assertThat("buffer durable increment(s) via proxy",
                new InstrumentedHBaseClient(underlying, "test").increment(req, true),
                is(resp));
    }

    @Test
    public void timesDurableIncrements() {
        final AtomicIncrementRequest req = mock(AtomicIncrementRequest.class);
        final Deferred<Long> resp = new Deferred<Long>();
        final Timer timer = registry.newTimer(
                underlying.getClass(), "increment", "requests");

        when(underlying.increment(req, true)).thenReturn(resp);
        when(metrics.getIncrements()).thenReturn(timer);

        new InstrumentedHBaseClient(underlying, metrics)
                .increment(req, true).callback(new Object());

        assertThat("times durable increment(s)",
                timer.max(), is(closeTo(50.0, 0.001)));
    }

    @Test
    public void proxiesCompareAndSets() {
        final PutRequest req = mock(PutRequest.class);
        final Deferred<Boolean> resp = new Deferred<Boolean>();
        when(underlying.compareAndSet(req, new byte[0])).thenReturn(resp);

        assertThat("compares and sets via proxy",
                new InstrumentedHBaseClient(underlying, "test")
                        .compareAndSet(req, new byte[0]),
                is(resp));
    }

    @Test
    public void timesCompareAndSets() {
        final PutRequest req = mock(PutRequest.class);
        final Deferred<Boolean> resp = new Deferred<Boolean>();
        final Timer timer = registry.newTimer(
                underlying.getClass(), "compareAndSet", "requests");

        when(underlying.compareAndSet(req, new byte[0])).thenReturn(resp);
        when(metrics.getCompareAndSets()).thenReturn(timer);

        new InstrumentedHBaseClient(underlying, metrics)
                .compareAndSet(req, new byte[0]).callback(new Object());

        assertThat("times compare and set(s)",
                timer.max(), is(closeTo(50.0, 0.001)));
    }

    @Test
    public void proxiesStringCompareAndSets() {
        final PutRequest req = mock(PutRequest.class);
        final Deferred<Boolean> resp = new Deferred<Boolean>();
        when(underlying.compareAndSet(req, "")).thenReturn(resp);

        assertThat("compares and sets Strings via proxy",
                new InstrumentedHBaseClient(underlying, "test").compareAndSet(req, ""),
                is(resp));
    }

    @Test
    public void timesStringCompareAndSets() {
        final PutRequest req = mock(PutRequest.class);
        final Deferred<Boolean> resp = new Deferred<Boolean>();
        final Timer timer = registry.newTimer(
                underlying.getClass(), "compareAndSet", "requests");

        when(underlying.compareAndSet(req, "")).thenReturn(resp);
        when(metrics.getCompareAndSets()).thenReturn(timer);

        new InstrumentedHBaseClient(underlying, metrics)
                .compareAndSet(req, "").callback(new Object());

        assertThat("times String compare and set(s)",
                timer.max(), is(closeTo(50.0, 0.001)));
    }

    @Test
    public void proxiesDeletes() {
        final DeleteRequest req = mock(DeleteRequest.class);
        final Deferred<Object> resp = new Deferred<Object>();
        when(underlying.delete(req)).thenReturn(resp);

        assertThat("deletes Strings via proxy",
                new InstrumentedHBaseClient(underlying, "test").delete(req), is(resp));
    }

    @Test
    public void timesDeletes() {
        final DeleteRequest req = mock(DeleteRequest.class);
        final Deferred<Object> resp = new Deferred<Object>();
        final Timer timer = registry.newTimer(
                underlying.getClass(), "delete", "requests");

        when(underlying.delete(req)).thenReturn(resp);
        when(metrics.getDeletes()).thenReturn(timer);

        new InstrumentedHBaseClient(underlying, metrics)
                .delete(req).callback(new Object());

        assertThat("times String compare and set(s)",
                timer.max(), is(closeTo(50.0, 0.001)));
    }

    @Test
    public void proxiesEnsureTableExists() {
        final Deferred<Object> resp = new Deferred<Object>();
        when(underlying.ensureTableExists(new byte[0])).thenReturn(resp);

        assertThat("ensures table exists via proxy",
                new InstrumentedHBaseClient(underlying, "test")
                        .ensureTableExists(new byte[0]),
                is(resp));
    }

    @Test
    public void timesEnsureTableExists() {
        final Deferred<Object> resp = new Deferred<Object>();
        final Timer timer = registry.newTimer(
                underlying.getClass(), "assertion", "requests");

        when(underlying.ensureTableExists(new byte[0])).thenReturn(resp);
        when(metrics.getAssertions()).thenReturn(timer);

        new InstrumentedHBaseClient(underlying, metrics)
                .ensureTableExists(new byte[0]).callback(new Object());

        assertThat("times ensure table exists",
                timer.max(), is(closeTo(50.0, 0.001)));
    }

    @Test
    public void proxiesEnsureStringTableExists() {
        final Deferred<Object> resp = new Deferred<Object>();
        when(underlying.ensureTableExists("")).thenReturn(resp);

        assertThat("ensures String table exists via proxy",
                new InstrumentedHBaseClient(underlying, "test").ensureTableExists(""),
                is(resp));
    }

    @Test
    public void timesEnsureStringTableExists() {
        final Deferred<Object> resp = new Deferred<Object>();
        final Timer timer = registry.newTimer(
                underlying.getClass(), "assertion", "requests");

        when(underlying.ensureTableExists("")).thenReturn(resp);
        when(metrics.getAssertions()).thenReturn(timer);

        new InstrumentedHBaseClient(underlying, metrics)
                .ensureTableExists("").callback(new Object());

        assertThat("times ensure String table exists",
                timer.max(), is(closeTo(50.0, 0.001)));
    }

    @Test
    public void proxiesFlushes() {
        final Deferred<Object> resp = new Deferred<Object>();
        when(underlying.flush()).thenReturn(resp);

        assertThat("flushes via proxy",
                new InstrumentedHBaseClient(underlying, "test").flush(), is(resp));
    }

    @Test
    public void timesFlushes() {
        final Deferred<Object> resp = new Deferred<Object>();
        final Timer timer = registry.newTimer(
                underlying.getClass(), "flush", "requests");

        when(underlying.flush()).thenReturn(resp);
        when(metrics.getFlushes()).thenReturn(timer);

        new InstrumentedHBaseClient(underlying, metrics)
                .flush().callback(new Object());

        assertThat("times flush(es)",
                timer.max(), is(closeTo(50.0, 0.001)));
    }

    @Test
    public void proxiesGets() {
        final GetRequest req = mock(GetRequest.class);
        final Deferred<ArrayList<KeyValue>> resp =
                new Deferred<ArrayList<KeyValue>>();
        when(underlying.get(req)).thenReturn(resp);

        assertThat("gets via proxy",
                new InstrumentedHBaseClient(underlying, "test").get(req), is(resp));
    }

    @Test
    public void timesGets() {
        final GetRequest req = mock(GetRequest.class);
        final Deferred<ArrayList<KeyValue>> resp =
                new Deferred<ArrayList<KeyValue>>();
        final Timer timer = registry.newTimer(
                underlying.getClass(), "get", "requests");

        when(underlying.get(req)).thenReturn(resp);
        when(metrics.getGets()).thenReturn(timer);

        new InstrumentedHBaseClient(underlying, metrics)
                .get(req).callback(new Object());

        assertThat("times get(s)",
                timer.max(), is(closeTo(50.0, 0.001)));
    }

    @Test
    public void proxiesLockRows() {
        final RowLockRequest req = mock(RowLockRequest.class);
        final Deferred<RowLock> resp = new Deferred<RowLock>();
        when(underlying.lockRow(req)).thenReturn(resp);

        assertThat("locks rows via proxy",
                new InstrumentedHBaseClient(underlying, "test").lockRow(req), is(resp));
    }

    @Test
    public void timesLockRows() {
        final RowLockRequest req = mock(RowLockRequest.class);
        final Deferred<RowLock> resp = new Deferred<RowLock>();
        final Timer timer = registry.newTimer(
                underlying.getClass(), "lock", "requests");

        when(underlying.lockRow(req)).thenReturn(resp);
        when(metrics.getLocks()).thenReturn(timer);

        new InstrumentedHBaseClient(underlying, metrics)
                .lockRow(req).callback(new Object());

        assertThat("times lock(s)",
                timer.max(), is(closeTo(50.0, 0.001)));
    }

    @Test
    public void proxiesPuts() {
        final PutRequest req = mock(PutRequest.class);
        final Deferred<Object> resp = new Deferred<Object>();
        when(underlying.put(req)).thenReturn(resp);

        assertThat("puts via proxy",
                new InstrumentedHBaseClient(underlying, "test").put(req), is(resp));
    }

    @Test
    public void timesPuts() {
        final PutRequest req = mock(PutRequest.class);
        final Deferred<Object> resp = new Deferred<Object>();
        final Timer timer = registry.newTimer(
                underlying.getClass(), "get", "requests");

        when(underlying.put(req)).thenReturn(resp);
        when(metrics.getPuts()).thenReturn(timer);

        new InstrumentedHBaseClient(underlying, metrics)
                .put(req).callback(new Object());

        assertThat("times put(s)",
                timer.max(), is(closeTo(50.0, 0.001)));
    }

    @Test
    public void proxiesShutdown() {
        final Deferred<Object> resp = new Deferred<Object>();
        when(underlying.shutdown()).thenReturn(resp);

        assertThat("shuts down via proxy",
                new InstrumentedHBaseClient(underlying, "test").shutdown(), is(resp));
    }

    @Test
    public void proxiesUnlockRows() {
        final RowLockRequest req = mock(RowLockRequest.class);
        final Deferred<RowLock> resp = new Deferred<RowLock>();
        when(underlying.lockRow(req)).thenReturn(resp);

        assertThat("unlocks rows via proxy",
                new InstrumentedHBaseClient(underlying, "test").lockRow(req), is(resp));
    }

    @Test
    public void timesUnlockRows() {
        final RowLockRequest req = mock(RowLockRequest.class);
        final Deferred<RowLock> resp = new Deferred<RowLock>();
        final Timer timer = registry.newTimer(
                underlying.getClass(), "lock", "requests");

        when(underlying.lockRow(req)).thenReturn(resp);
        when(metrics.getLocks()).thenReturn(timer);

        new InstrumentedHBaseClient(underlying, metrics)
                .lockRow(req).callback(new Object());

        assertThat("times unlock(s)",
                timer.max(), is(closeTo(50.0, 0.001)));
    }
}
