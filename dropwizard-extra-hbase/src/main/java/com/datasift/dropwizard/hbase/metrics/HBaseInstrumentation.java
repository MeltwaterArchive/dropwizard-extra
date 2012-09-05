package com.datasift.dropwizard.hbase.metrics;

import com.datasift.dropwizard.hbase.HBaseClient;
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
        final Class<? extends HBaseClient> clazz = client.getClass();
        
        // timers
        creates        = registry.newTimer(clazz, "create",        "requests");
        increments     = registry.newTimer(clazz, "increment",     "requests");
        compareAndSets = registry.newTimer(clazz, "compareAndSet", "requests");
        deletes        = registry.newTimer(clazz, "delete",        "requests");
        assertions     = registry.newTimer(clazz, "assertion",     "requests");
        flushes        = registry.newTimer(clazz, "flush",         "requests");
        gets           = registry.newTimer(clazz, "get",           "requests");
        locks          = registry.newTimer(clazz, "lock",          "requests");
        puts           = registry.newTimer(clazz, "put",           "requests");
        unlocks        = registry.newTimer(clazz, "unlock",        "requests");
        scans          = registry.newTimer(clazz, "scans",         "scanner");
        closes         = registry.newTimer(clazz, "closes",        "scanner");

        // client stats
        registry.newGauge(clazz, "atomicIncrements", "totals", new Gauge<Long>() {
            @Override public Long value() {
                return client.stats().atomicIncrements();
            }
        });
        registry.newGauge(clazz, "connectionsCreated", "totals", new Gauge<Long>() {
            @Override public Long value() {
                return client.stats().connectionsCreated();
            }
        });
        registry.newGauge(clazz, "contendedMetaLookups", "totals", new Gauge<Long>() {
            @Override public Long value() {
                return client.stats().contendedMetaLookups();
            }
        });
        registry.newGauge(clazz, "deletes", "totals", new Gauge<Long>() {
            @Override public Long value() {
                return client.stats().deletes();
            }
        });
        registry.newGauge(clazz, "flushes", "totals", new Gauge<Long>() {
            @Override public Long value() {
                return client.stats().flushes();
            }
        });
        registry.newGauge(clazz, "gets", "totals", new Gauge<Long>() {
            @Override public Long value() {
                return client.stats().gets();
            }
        });
        registry.newGauge(clazz, "noSuchRegionExceptions", "totals", new Gauge<Long>() {
            @Override public Long value() {
                return client.stats().noSuchRegionExceptions();
            }
        });
        registry.newGauge(clazz, "numBatchedRpcSent", "totals", new Gauge<Long>() {
            @Override public Long value() {
                return client.stats().numBatchedRpcSent();
            }
        });
        registry.newGauge(clazz, "numRpcDelayedDueToNSRE", "totals", new Gauge<Long>() {
            @Override public Long value() {
                return client.stats().numRpcDelayedDueToNSRE();
            }
        });
        registry.newGauge(clazz, "puts", "totals", new Gauge<Long>() {
            @Override public Long value() {
                return client.stats().puts();
            }
        });
        registry.newGauge(clazz, "rootLookups", "totals", new Gauge<Long>() {
            @Override public Long value() {
                return client.stats().rootLookups();
            }
        });
        registry.newGauge(clazz, "rowLocks", "totals", new Gauge<Long>() {
            @Override public Long value() {
                return client.stats().rowLocks();
            }
        });
        registry.newGauge(clazz, "scannersOpened", "totals", new Gauge<Long>() {
            @Override public Long value() {
                return client.stats().scannersOpened();
            }
        });
        registry.newGauge(clazz, "scans", "totals", new Gauge<Long>() {
            @Override public Long value() {
                return client.stats().scans();
            }
        });
        registry.newGauge(clazz, "uncontendedMetaLookups", "totals", new Gauge<Long>() {
            @Override public Long value() {
                return client.stats().uncontendedMetaLookups();
            }
        });

        // increment buffer stats
        registry.newGauge(clazz, "averageLoadPenalty", "incrementBuffer",
                new Gauge<Double>() {
                    @Override public Double value() {
                        return client.stats().incrementBufferStats()
                                .averageLoadPenalty();
                    }
                });
        registry.newGauge(clazz, "evictionCount", "incrementBuffer",
                new Gauge<Long>() {
                    @Override public Long value() {
                        return client.stats().incrementBufferStats()
                                .evictionCount();
                    }
                });
        registry.newGauge(clazz, "hitCount", "incrementBuffer",
                new Gauge<Long>() {
                    @Override public Long value() {
                        return client.stats().incrementBufferStats().hitCount();
                    }
                });
        registry.newGauge(clazz, "hitRate", "incrementBuffer",
                new Gauge<Double>() {
                    @Override public Double value() {
                        return client.stats().incrementBufferStats().hitRate();
                    }
                });
        registry.newGauge(clazz, "loadCount", "incrementBuffer",
                new Gauge<Long>() {
                    @Override public Long value() {
                        return client.stats().incrementBufferStats()
                                .loadCount();
                    }
                });
        registry.newGauge(clazz, "loadExceptionCount", "incrementBuffer",
                new Gauge<Long>() {
                    @Override public Long value() {
                        return client.stats().incrementBufferStats()
                                .loadExceptionCount();
                    }
                });
        registry.newGauge(clazz, "loadExceptionRate", "incrementBuffer",
                new Gauge<Double>() {
                    @Override public Double value() {
                        return client.stats().incrementBufferStats()
                                .loadExceptionRate();
                    }
                });
        registry.newGauge(clazz, "loadSuccessCount", "incrementBuffer",
                new Gauge<Long>() {
                    @Override public Long value() {
                        return client.stats().incrementBufferStats()
                                .loadSuccessCount();
                    }
                });
        registry.newGauge(clazz, "missCount", "incrementBuffer",
                new Gauge<Long>() {
                    @Override public Long value() {
                        return client.stats().incrementBufferStats()
                                .missCount();
                    }
                });
        registry.newGauge(clazz, "missRate", "incrementBuffer",
                new Gauge<Double>() {
                    @Override public Double value() {
                        return client.stats().incrementBufferStats()
                                .missRate();
                    }
                });
        registry.newGauge(clazz, "requestCount", "incrementBuffer",
                new Gauge<Long>() {
                    @Override public Long value() {
                        return client.stats().incrementBufferStats()
                                .requestCount();
                    }
                });
        registry.newGauge(clazz, "totalLoadTime", "incrementBuffer",
                new Gauge<Long>() {
                    @Override public Long value() {
                        return client.stats().incrementBufferStats()
                                .totalLoadTime();
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
