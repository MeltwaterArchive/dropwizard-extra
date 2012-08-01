package com.datasift.dropwizard.hbase.metrics;

import com.datasift.dropwizard.hbase.HBaseClient;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;

/**
 * A container for {@link Timer}s used to time {@link HBaseClient} requests.
 *
 * @see com.datasift.dropwizard.hbase.InstrumentedHBaseClient
 */
public class HBaseInstrumentation {

    // request timers
    private final Timer creates;
    private final Timer increments;
    private final Timer compareAndSets;
    private final Timer deletes;
    private final Timer assertions;
    private final Timer flushes;
    private final Timer gets;
    private final Timer locks;
    private final Timer puts;
    private final Timer unlocks;
    private final Timer scans;
    private final Timer closes;

    public HBaseInstrumentation(final HBaseClient client,
                                final MetricsRegistry registry) {
        // timers
        creates        = registry.newTimer(
                client.getClass(), "create",        "requests");
        increments     = registry.newTimer(
                client.getClass(), "increment",     "requests");
        compareAndSets = registry.newTimer(
                client.getClass(), "compareAndSet", "requests");
        deletes        = registry.newTimer(
                client.getClass(), "delete",        "requests");
        assertions     = registry.newTimer(
                client.getClass(), "assertion",     "requests");
        flushes        = registry.newTimer(
                client.getClass(), "flush",         "requests");
        gets           = registry.newTimer(
                client.getClass(), "get",           "requests");
        locks          = registry.newTimer(
                client.getClass(), "lock",          "requests");
        puts           = registry.newTimer(
                client.getClass(), "put",           "requests");
        unlocks        = registry.newTimer(
                client.getClass(), "unlock",        "requests");
        scans          = registry.newTimer(
                client.getClass(), "scans",         "scanner");
        closes         = registry.newTimer(
                client.getClass(), "closes",        "scanner");

        // client stats
        registry.newGauge(getClass(), "atomicIncrements", new Gauge<Long>() {
            @Override
            public Long value() {
                return client.stats().atomicIncrements();
            }
        });
        registry.newGauge(getClass(), "connectionsCreated", new Gauge<Long>() {
            @Override
            public Long value() {
                return client.stats().connectionsCreated();
            }
        });
        registry.newGauge(getClass(), "contendedMetaLookups", new Gauge<Long>() {
            @Override
            public Long value() {
                return client.stats().contendedMetaLookups();
            }
        });
        registry.newGauge(getClass(), "deletes", new Gauge<Long>() {
            @Override
            public Long value() {
                return client.stats().deletes();
            }
        });
        registry.newGauge(getClass(), "flushes", new Gauge<Long>() {
            @Override
            public Long value() {
                return client.stats().flushes();
            }
        });
        registry.newGauge(getClass(), "gets", new Gauge<Long>() {
            @Override
            public Long value() {
                return client.stats().gets();
            }
        });
        registry.newGauge(getClass(), "noSuchRegionExceptions", new Gauge<Long>() {
            @Override
            public Long value() {
                return client.stats().noSuchRegionExceptions();
            }
        });
        registry.newGauge(getClass(), "numBatchedRpcSent", new Gauge<Long>() {
            @Override
            public Long value() {
                return client.stats().numBatchedRpcSent();
            }
        });
        registry.newGauge(getClass(), "numRpcDelayedDueToNSRE", new Gauge<Long>() {
            @Override
            public Long value() {
                return client.stats().numRpcDelayedDueToNSRE();
            }
        });
        registry.newGauge(getClass(), "puts", new Gauge<Long>() {
            @Override
            public Long value() {
                return client.stats().puts();
            }
        });
        registry.newGauge(getClass(), "rootLookups", new Gauge<Long>() {
            @Override
            public Long value() {
                return client.stats().rootLookups();
            }
        });
        registry.newGauge(getClass(), "rowLocks", new Gauge<Long>() {
            @Override
            public Long value() {
                return client.stats().rowLocks();
            }
        });
        registry.newGauge(getClass(), "scannersOpened", new Gauge<Long>() {
            @Override
            public Long value() {
                return client.stats().scannersOpened();
            }
        });
        registry.newGauge(getClass(), "scans", new Gauge<Long>() {
            @Override
            public Long value() {
                return client.stats().scans();
            }
        });
        registry.newGauge(getClass(), "uncontendedMetaLookups", new Gauge<Long>() {
            @Override
            public Long value() {
                return client.stats().uncontendedMetaLookups();
            }
        });

        // increment buffer stats
        registry.newGauge(getClass(), "averageLoadPenalty", "incrementBuffer", new Gauge<Double>() {
            @Override
            public Double value() {
                return client.stats().incrementBufferStats().averageLoadPenalty();
            }
        });
        registry.newGauge(getClass(), "evictionCount", "incrementBuffer", new Gauge<Long>() {
            @Override
            public Long value() {
                return client.stats().incrementBufferStats().evictionCount();
            }
        });
        registry.newGauge(getClass(), "hitCount", "incrementBuffer", new Gauge<Long>() {
            @Override
            public Long value() {
                return client.stats().incrementBufferStats().hitCount();
            }
        });
        registry.newGauge(getClass(), "hitRate", "incrementBuffer", new Gauge<Double>() {
            @Override
            public Double value() {
                return client.stats().incrementBufferStats().hitRate();
            }
        });
        registry.newGauge(getClass(), "loadCount", "incrementBuffer", new Gauge<Long>() {
            @Override
            public Long value() {
                return client.stats().incrementBufferStats().loadCount();
            }
        });
        registry.newGauge(getClass(), "loadExceptionCount", "incrementBuffer", new Gauge<Long>() {
            @Override
            public Long value() {
                return client.stats().incrementBufferStats().loadExceptionCount();
            }
        });
        registry.newGauge(getClass(), "loadExceptionRate", "incrementBuffer", new Gauge<Double>() {
            @Override
            public Double value() {
                return client.stats().incrementBufferStats().loadExceptionRate();
            }
        });
        registry.newGauge(getClass(), "loadSuccessCount", "incrementBuffer", new Gauge<Long>() {
            @Override
            public Long value() {
                return client.stats().incrementBufferStats().loadSuccessCount();
            }
        });
        registry.newGauge(getClass(), "missCount", "incrementBuffer", new Gauge<Long>() {
            @Override
            public Long value() {
                return client.stats().incrementBufferStats().missCount();
            }
        });
        registry.newGauge(getClass(), "missRate", "incrementBuffer", new Gauge<Double>() {
            @Override
            public Double value() {
                return client.stats().incrementBufferStats().missRate();
            }
        });
        registry.newGauge(getClass(), "requestCount", "incrementBuffer", new Gauge<Long>() {
            @Override
            public Long value() {
                return client.stats().incrementBufferStats().requestCount();
            }
        });
        registry.newGauge(getClass(), "totalLoadTime", "incrementBuffer", new Gauge<Long>() {
            @Override
            public Long value() {
                return client.stats().incrementBufferStats().totalLoadTime();
            }
        });
    }

    public Timer getCreates() {
        return creates;
    }

    public Timer getIncrements() {
        return increments;
    }

    public Timer getCompareAndSets() {
        return compareAndSets;
    }

    public Timer getDeletes() {
        return deletes;
    }

    public Timer getAssertions() {
        return assertions;
    }

    public Timer getFlushes() {
        return flushes;
    }

    public Timer getGets() {
        return gets;
    }

    public Timer getLocks() {
        return locks;
    }

    public Timer getPuts() {
        return puts;
    }

    public Timer getUnlocks() {
        return unlocks;
    }

    public Timer getScans() {
        return scans;
    }

    public Timer getCloses() {
        return closes;
    }
}
