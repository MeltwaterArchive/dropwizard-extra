package com.datasift.dropwizard.hbase.metrics;

import com.datasift.dropwizard.hbase.HBaseClient;
import com.datasift.dropwizard.hbase.scanner.RowScanner;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

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
    private final MetricRegistry registry;

    /**
     * Initialises instrumentation for the given {@link HBaseClient} using the given {@link
     * com.codahale.metrics.MetricRegistry}.
     *
     * @param client   the client to create metrics for.
     * @param registry the registry to register the metrics with.
     * @param name     the name of the client/scanner to register metrics under.
     */
    public HBaseInstrumentation(final HBaseClient client,
                                final MetricRegistry registry,
                                final String name) {
        this.name = name;
        this.registry = registry;
        
        // timers
        creates        = registry.timer(MetricRegistry.name(name, "create"));
        increments     = registry.timer(MetricRegistry.name(name, "increment"));
        compareAndSets = registry.timer(MetricRegistry.name(name, "compareAndSet"));
        deletes        = registry.timer(MetricRegistry.name(name, "delete"));
        assertions     = registry.timer(MetricRegistry.name(name, "assertion"));
        flushes        = registry.timer(MetricRegistry.name(name, "flush"));
        gets           = registry.timer(MetricRegistry.name(name, "get"));
        locks          = registry.timer(MetricRegistry.name(name, "lock"));
        puts           = registry.timer(MetricRegistry.name(name, "put"));
        unlocks        = registry.timer(MetricRegistry.name(name, "unlock"));

        // client stats
        registry.register(MetricRegistry.name(name, "totals", "atomicIncrements"),
                new Gauge<Long>() {
                    @Override public Long getValue() {
                        return client.stats().atomicIncrements();
                    }
                });
        registry.register(MetricRegistry.name(name, "totals", "connectionsCreated"),
                new Gauge<Long>() {
                    @Override public Long getValue() {
                        return client.stats().connectionsCreated();
                    }
                });
        registry.register(MetricRegistry.name(name, "totals", "contendedMetaLookups"),
                new Gauge<Long>() {
                    @Override public Long getValue() {
                        return client.stats().contendedMetaLookups();
                    }
                });
        registry.register(MetricRegistry.name(name, "totals", "deletes"),
                new Gauge<Long>() {
                    @Override public Long getValue() {
                        return client.stats().deletes();
                    }
                });
        registry.register(MetricRegistry.name(name, "totals", "flushes"),
                new Gauge<Long>() {
                    @Override public Long getValue() {
                        return client.stats().flushes();
                    }
                });
        registry.register(MetricRegistry.name(name, "totals", "gets"),
                new Gauge<Long>() {
                    @Override public Long getValue() {
                        return client.stats().gets();
                    }
                });
        registry.register(MetricRegistry.name(name, "totals", "noSuchRegionExceptions"),
                new Gauge<Long>() {
                    @Override public Long getValue() {
                        return client.stats().noSuchRegionExceptions();
                    }
                });
        registry.register(MetricRegistry.name(name, "totals", "numBatchedRpcSent"),
                new Gauge<Long>() {
                    @Override public Long getValue() {
                        return client.stats().numBatchedRpcSent();
                    }
                });
        registry.register(MetricRegistry.name(name, "totals", "numRpcDelayedDueToNSRE"),
                new Gauge<Long>() {
                    @Override public Long getValue() {
                        return client.stats().numRpcDelayedDueToNSRE();
                    }
                });
        registry.register(MetricRegistry.name(name, "totals", "puts"),
                new Gauge<Long>() {
                    @Override public Long getValue() {
                        return client.stats().puts();
                    }
                });
        registry.register(MetricRegistry.name(name, "totals", "rootLookups"),
                new Gauge<Long>() {
                    @Override public Long getValue() {
                        return client.stats().rootLookups();
                    }
                });
        registry.register(MetricRegistry.name(name, "totals", "rowLocks"),
                new Gauge<Long>() {
                    @Override public Long getValue() {
                        return client.stats().rowLocks();
                    }
                });
        registry.register(MetricRegistry.name(name, "totals", "scannersOpened"),
                new Gauge<Long>() {
                    @Override public Long getValue() {
                        return client.stats().scannersOpened();
                    }
                });
        registry.register(MetricRegistry.name(name, "totals", "scans"),
                new Gauge<Long>() {
                    @Override public Long getValue() {
                        return client.stats().scans();
                    }
                });
        registry.register(MetricRegistry.name(name, "totals", "uncontendedMetaLookups"),
                new Gauge<Long>() {
                    @Override public Long getValue() {
                        return client.stats().uncontendedMetaLookups();
                    }
                });

        // increment buffer stats
        registry.register(MetricRegistry.name(name, "incrementBuffer", "averageLoadPenalty"),
                new Gauge<Double>() {
                    @Override public Double getValue() {
                        return client.stats().incrementBufferStats()
                                .averageLoadPenalty();
                    }
                });
        registry.register(MetricRegistry.name(name, "incrementBuffer", "evictionCount"),
                new Gauge<Long>() {
                    @Override public Long getValue() {
                        return client.stats().incrementBufferStats()
                                .evictionCount();
                    }
                });
        registry.register(MetricRegistry.name(name, "incrementBuffer", "hitCount"),
                new Gauge<Long>() {
                    @Override public Long getValue() {
                        return client.stats().incrementBufferStats().hitCount();
                    }
                });
        registry.register(MetricRegistry.name(name, "incrementBuffer", "hitRate"),
                new Gauge<Double>() {
                    @Override public Double getValue() {
                        return client.stats().incrementBufferStats().hitRate();
                    }
                });
        registry.register(MetricRegistry.name(name, "incrementBuffer", "loadCount"),
                new Gauge<Long>() {
                    @Override public Long getValue() {
                        return client.stats().incrementBufferStats()
                                .loadCount();
                    }
                });
        registry.register(MetricRegistry.name(name, "incrementBuffer", "loadExceptionCount"),
                new Gauge<Long>() {
                    @Override public Long getValue() {
                        return client.stats().incrementBufferStats()
                                .loadExceptionCount();
                    }
                });
        registry.register(MetricRegistry.name(name, "incrementBuffer", "loadExceptionRate"),
                new Gauge<Double>() {
                    @Override public Double getValue() {
                        return client.stats().incrementBufferStats()
                                .loadExceptionRate();
                    }
                });
        registry.register(MetricRegistry.name(name, "incrementBuffer", "loadSuccessCount"),
                new Gauge<Long>() {
                    @Override public Long getValue() {
                        return client.stats().incrementBufferStats()
                                .loadSuccessCount();
                    }
                });
        registry.register(MetricRegistry.name(name, "incrementBuffer", "missCount"),
                new Gauge<Long>() {
                    @Override public Long getValue() {
                        return client.stats().incrementBufferStats()
                                .missCount();
                    }
                });
        registry.register(MetricRegistry.name(name, "incrementBuffer", "missRate"),
                new Gauge<Double>() {
                    @Override public Double getValue() {
                        return client.stats().incrementBufferStats()
                                .missRate();
                    }
                });
        registry.register(MetricRegistry.name(name, "incrementBuffer", "requestCount"),
                new Gauge<Long>() {
                    @Override public Long getValue() {
                        return client.stats().incrementBufferStats()
                                .requestCount();
                    }
                });
        registry.register(MetricRegistry.name(name, "incrementBuffer", "totalLoadTime"),
                new Gauge<Long>() {
                    @Override public Long getValue() {
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
