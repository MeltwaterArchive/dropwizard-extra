package com.datasift.dropwizard.hbase;

import com.datasift.dropwizard.hbase.config.HBaseClientConfiguration;
import com.datasift.dropwizard.hbase.scanner.InstrumentedRowScanner;
import com.datasift.dropwizard.hbase.scanner.RowScanner;
import com.datasift.dropwizard.hbase.util.TimerStoppingCallback;
import com.stumbleupon.async.Deferred;
import com.yammer.dropwizard.util.Duration;
import com.yammer.dropwizard.util.Size;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.hbase.async.*;

import java.util.ArrayList;

/**
 * An {@link HBaseClient} that is instrumented with {@link Metrics}.
 */
public class InstrumentedHBaseClient implements HBaseClient {

    private HBaseClient client;
    private MetricsRegistry registry;

    // request timers
    private Timer creates        = registry.newTimer(getClass(), "create", "requests");
    private Timer increments     = registry.newTimer(getClass(), "increment", "requests");
    private Timer compareAndSets = registry.newTimer(getClass(), "compareAndSet", "requests");
    private Timer deletes        = registry.newTimer(getClass(), "delete", "requests");
    private Timer assertions     = registry.newTimer(getClass(), "assertion", "requests");
    private Timer flushes        = registry.newTimer(getClass(), "flush", "requests");
    private Timer gets           = registry.newTimer(getClass(), "get", "requests");
    private Timer locks          = registry.newTimer(getClass(), "lock", "requests");
    private Timer puts           = registry.newTimer(getClass(), "put", "requests");
    private Timer unlocks        = registry.newTimer(getClass(), "unlock", "requests");

    public static HBaseClient wrap(HBaseClientConfiguration configuration, HBaseClient client) {
        if (configuration.isInstrumented()) {
            return new InstrumentedHBaseClient(client);
        } else {
            return client;
        }
    }

    public InstrumentedHBaseClient(HBaseClient client) {
        this(client, Metrics.defaultRegistry());
    }

    public InstrumentedHBaseClient(HBaseClient client, MetricsRegistry registry) {
        this.client = client;
        this.registry = registry;
        initGauges();
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
        TimerContext ctx = creates.time();
        return client.create(edit).addBoth(new TimerStoppingCallback<Boolean>(ctx));
    }

    public Deferred<Long> bufferIncrement(AtomicIncrementRequest request) {
        TimerContext ctx = creates.time();
        return client.bufferIncrement(request).addBoth(new TimerStoppingCallback<Long>(ctx));
    }

    public Deferred<Long> increment(AtomicIncrementRequest request) {
        TimerContext ctx = creates.time();
        return client.increment(request).addBoth(new TimerStoppingCallback<Long>(ctx));
    }

    public Deferred<Long> increment(AtomicIncrementRequest request, Boolean durable) {
        TimerContext ctx = creates.time();
        return client.increment(request, durable).addBoth(new TimerStoppingCallback<Long>(ctx));
    }

    public Deferred<Boolean> compareAndSet(PutRequest edit, byte[] expected) {
        TimerContext ctx = creates.time();
        return client.compareAndSet(edit, expected).addBoth(new TimerStoppingCallback<Boolean>(ctx));
    }

    public Deferred<Boolean> compareAndSet(PutRequest edit, String expected) {
        TimerContext ctx = creates.time();
        return client.compareAndSet(edit, expected).addBoth(new TimerStoppingCallback<Boolean>(ctx));
    }

    public Deferred<Object> delete(DeleteRequest request) {
        TimerContext ctx = creates.time();
        return client.delete(request).addBoth(new TimerStoppingCallback<Object>(ctx));
    }

    public Deferred<Object> ensureTableExists(byte[] table) {
        TimerContext ctx = creates.time();
        return client.ensureTableExists(table).addBoth(new TimerStoppingCallback<Object>(ctx));
    }

    public Deferred<Object> ensureTableExists(String table) {
        TimerContext ctx = creates.time();
        return client.ensureTableExists(table).addBoth(new TimerStoppingCallback<Object>(ctx));
    }

    public Deferred<Object> ensureTableFamilyExists(byte[] table, byte[] family) {
        TimerContext ctx = creates.time();
        return client.ensureTableFamilyExists(table, family).addBoth(new TimerStoppingCallback<Object>(ctx));
    }

    public Deferred<Object> ensureTableFamilyExists(String table, String family) {
        TimerContext ctx = creates.time();
        return client.ensureTableFamilyExists(table, family).addBoth(new TimerStoppingCallback<Object>(ctx));
    }

    public Deferred<Object> flush() {
        TimerContext ctx = creates.time();
        return client.flush().addBoth(new TimerStoppingCallback<Object>(ctx));
    }

    public Deferred<ArrayList<KeyValue>> get(GetRequest request) {
        TimerContext ctx = creates.time();
        return client.get(request).addBoth(new TimerStoppingCallback<ArrayList<KeyValue>>(ctx));
    }

    public Deferred<RowLock> lockRow(RowLockRequest request) {
        TimerContext ctx = creates.time();
        return client.lockRow(request).addBoth(new TimerStoppingCallback<RowLock>(ctx));
    }

    public RowScanner newScanner(byte[] table) {
        return new InstrumentedRowScanner(client.newScanner(table), registry);
    }

    public RowScanner newScanner(String table) {
        return new InstrumentedRowScanner(client.newScanner(table), registry);
    }

    public Deferred<Object> put(PutRequest request) {
        TimerContext ctx = creates.time();
        return client.put(request).addBoth(new TimerStoppingCallback<Object>(ctx));
    }

    public Deferred<Object> shutdown() {
        TimerContext ctx = creates.time();
        return client.shutdown().addBoth(new TimerStoppingCallback<Object>(ctx));
    }

    public ClientStats stats() {
        return client.stats();
    }

    public org.jboss.netty.util.Timer getTimer() {
        return client.getTimer();
    }

    public Deferred<Object> unlockRow(RowLock lock) {
        TimerContext ctx = creates.time();
        return client.unlockRow(lock).addBoth(new TimerStoppingCallback<Object>(ctx));
    }

    protected void initGauges() {
        // client stats
        registry.newGauge(getClass(), "atomicIncrements", new Gauge<Long>() {
            @Override
            public Long value() {
                return stats().atomicIncrements();
            }
        });
        registry.newGauge(getClass(), "connectionsCreated", new Gauge<Long>() {
            @Override
            public Long value() {
                return stats().connectionsCreated();
            }
        });
        registry.newGauge(getClass(), "contendedMetaLookups", new Gauge<Long>() {
            @Override
            public Long value() {
                return stats().contendedMetaLookups();
            }
        });
        registry.newGauge(getClass(), "deletes", new Gauge<Long>() {
            @Override
            public Long value() {
                return stats().deletes();
            }
        });
        registry.newGauge(getClass(), "flushes", new Gauge<Long>() {
            @Override
            public Long value() {
                return stats().flushes();
            }
        });
        registry.newGauge(getClass(), "gets", new Gauge<Long>() {
            @Override
            public Long value() {
                return stats().gets();
            }
        });
        registry.newGauge(getClass(), "noSuchRegionExceptions", new Gauge<Long>() {
            @Override
            public Long value() {
                return stats().noSuchRegionExceptions();
            }
        });
        registry.newGauge(getClass(), "numBatchedRpcSent", new Gauge<Long>() {
            @Override
            public Long value() {
                return stats().numBatchedRpcSent();
            }
        });
        registry.newGauge(getClass(), "numRpcDelayedDueToNSRE", new Gauge<Long>() {
            @Override
            public Long value() {
                return stats().numRpcDelayedDueToNSRE();
            }
        });
        registry.newGauge(getClass(), "puts", new Gauge<Long>() {
            @Override
            public Long value() {
                return stats().puts();
            }
        });
        registry.newGauge(getClass(), "rootLookups", new Gauge<Long>() {
            @Override
            public Long value() {
                return stats().rootLookups();
            }
        });
        registry.newGauge(getClass(), "rowLocks", new Gauge<Long>() {
            @Override
            public Long value() {
                return stats().rowLocks();
            }
        });
        registry.newGauge(getClass(), "scannersOpened", new Gauge<Long>() {
            @Override
            public Long value() {
                return stats().scannersOpened();
            }
        });
        registry.newGauge(getClass(), "scans", new Gauge<Long>() {
            @Override
            public Long value() {
                return stats().scans();
            }
        });
        registry.newGauge(getClass(), "uncontendedMetaLookups", new Gauge<Long>() {
            @Override
            public Long value() {
                return stats().uncontendedMetaLookups();
            }
        });

        // increment buffer stats
        registry.newGauge(getClass(), "averageLoadPenalty", "incrementBuffer", new Gauge<Double>() {
            @Override
            public Double value() {
                return stats().incrementBufferStats().averageLoadPenalty();
            }
        });
        registry.newGauge(getClass(), "evictionCount", "incrementBuffer", new Gauge<Long>() {
            @Override
            public Long value() {
                return stats().incrementBufferStats().evictionCount();
            }
        });
        registry.newGauge(getClass(), "hitCount", "incrementBuffer", new Gauge<Long>() {
            @Override
            public Long value() {
                return stats().incrementBufferStats().hitCount();
            }
        });
        registry.newGauge(getClass(), "hitRate", "incrementBuffer", new Gauge<Double>() {
            @Override
            public Double value() {
                return stats().incrementBufferStats().hitRate();
            }
        });
        registry.newGauge(getClass(), "loadCount", "incrementBuffer", new Gauge<Long>() {
            @Override
            public Long value() {
                return stats().incrementBufferStats().loadCount();
            }
        });
        registry.newGauge(getClass(), "loadExceptionCount", "incrementBuffer", new Gauge<Long>() {
            @Override
            public Long value() {
                return stats().incrementBufferStats().loadExceptionCount();
            }
        });
        registry.newGauge(getClass(), "loadExceptionRate", "incrementBuffer", new Gauge<Double>() {
            @Override
            public Double value() {
                return stats().incrementBufferStats().loadExceptionRate();
            }
        });
        registry.newGauge(getClass(), "loadSuccessCount", "incrementBuffer", new Gauge<Long>() {
            @Override
            public Long value() {
                return stats().incrementBufferStats().loadSuccessCount();
            }
        });
        registry.newGauge(getClass(), "missCount", "incrementBuffer", new Gauge<Long>() {
            @Override
            public Long value() {
                return stats().incrementBufferStats().missCount();
            }
        });
        registry.newGauge(getClass(), "missRate", "incrementBuffer", new Gauge<Double>() {
            @Override
            public Double value() {
                return stats().incrementBufferStats().missRate();
            }
        });
        registry.newGauge(getClass(), "requestCount", "incrementBuffer", new Gauge<Long>() {
            @Override
            public Long value() {
                return stats().incrementBufferStats().requestCount();
            }
        });
        registry.newGauge(getClass(), "totalLoadTime", "incrementBuffer", new Gauge<Long>() {
            @Override
            public Long value() {
                return stats().incrementBufferStats().totalLoadTime();
            }
        });

    }
}
