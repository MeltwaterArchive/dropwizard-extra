package com.datasift.dropwizard.hbase.metrics;

import com.datasift.dropwizard.hbase.HBaseClient;
import com.datasift.dropwizard.hbase.scanner.RowScanner;
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

    private final String name;
    private final MetricsRegistry registry;

    /**
     * Initialises instrumentation for the given {@link HBaseClient} using the given {@link
     * MetricsRegistry}.
     *
     * @param client   the client to create metrics for.
     * @param registry the registry to register the metrics with.
     * @param name     the name of the client/scanner to register metrics under.
     */
    public HBaseInstrumentation(final HBaseClient client,
                                final MetricsRegistry registry,
                                final String name) {
        final Class<? extends HBaseClient> clazz = client.getClass();

        this.name = name;
        this.registry = registry;

        // timers
        creates        = registry.newTimer(clazz, "create",        name);
        increments     = registry.newTimer(clazz, "increment",     name);
        compareAndSets = registry.newTimer(clazz, "compareAndSet", name);
        deletes        = registry.newTimer(clazz, "delete",        name);
        assertions     = registry.newTimer(clazz, "assertion",     name);
        flushes        = registry.newTimer(clazz, "flush",         name);
        gets           = registry.newTimer(clazz, "get",           name);
        locks          = registry.newTimer(clazz, "lock",          name);
        puts           = registry.newTimer(clazz, "put",           name);
        unlocks        = registry.newTimer(clazz, "unlock",        name);

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

    /**
     * Gets the {@link Timer} for create requests.
     *
     * @return the {@link Timer} for create requests.
     */
    public Timer getCreates() {
        return creates;
    }

    /**
     * Gets the {@link Timer} for increment requests.
     *
     * @return the {@link Timer} for increment requests.
     */
    public Timer getIncrements() {
        return increments;
    }

    /**
     * Gets the {@link Timer} for compare-and-set requests.
     *
     * @return the {@link Timer} for compare-and-set requests.
     */
    public Timer getCompareAndSets() {
        return compareAndSets;
    }

    /**
     * Gets the {@link Timer} for delete requests.
     *
     * @return the {@link Timer} for delete requests.
     */
    public Timer getDeletes() {
        return deletes;
    }

    /**
     * Gets the {@link Timer} for assertion requests.
     *
     * @return the {@link Timer} for assertion requests.
     */
    public Timer getAssertions() {
        return assertions;
    }

    /**
     * Gets the {@link Timer} for flush requests.
     *
     * @return the {@link Timer} for flush requests.
     */
    public Timer getFlushes() {
        return flushes;
    }

    /**
     * Gets the {@link Timer} for get requests.
     *
     * @return the {@link Timer} for get requests.
     */
    public Timer getGets() {
        return gets;
    }

    /**
     * Gets the {@link Timer} for lock requests.
     *
     * @return the {@link Timer} for lock requests.
     */
    public Timer getLocks() {
        return locks;
    }

    /**
     * Gets the {@link Timer} for put requests.
     *
     * @return the {@link Timer} for put requests.
     */
    public Timer getPuts() {
        return puts;
    }

    /**
     * Gets the {@link Timer} for unlock requests.
     *
     * @return the {@link Timer} for unlock requests.
     */
    public Timer getUnlocks() {
        return unlocks;
    }

    /**
     * Creates the instrumentation for a {@link RowScanner}.
     *
     * @param scanner The {@link RowScanner} to instrument with metrics.
     *
     * @return The instrumentation for the {@link RowScanner}.
     */
    public ScannerInstrumentation instrument(final RowScanner scanner) {
        return new ScannerInstrumentation(scanner, registry, name);
    }
}
