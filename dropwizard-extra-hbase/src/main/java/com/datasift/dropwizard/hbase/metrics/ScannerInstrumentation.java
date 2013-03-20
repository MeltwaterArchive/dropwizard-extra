package com.datasift.dropwizard.hbase.metrics;

import com.datasift.dropwizard.hbase.scanner.RowScanner;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;

/**
 * A container for {@link Timer}s used to time {@link RowScanner} requests.
 *
 * @see com.datasift.dropwizard.hbase.scanner.RowScanner
 */
public class ScannerInstrumentation {

    private final Timer scans;
    private final Timer closes;

    /**
     * Initialises instrumentation for the given {@link RowScanner} using the given {@link
     * MetricsRegistry}.
     *
     * @param scanner the scanner to create metrics for.
     * @param registry the registry to register the metrics with.
     * @param name the name of the client/scanner to register metrics under.
     */

    ScannerInstrumentation(final RowScanner scanner,
                           final MetricsRegistry registry,
                           final String name) {
        final Class<? extends RowScanner> clazz = scanner.getClass();

        scans = registry.newTimer(clazz, "scans", name);
        closes = registry.newTimer(clazz, "closes", name);
    }

    /**
     * Gets the {@link Timer} for scan requests.
     *
     * @return the {@link Timer} for scan requests.
     */
    public Timer getScans() {
        return scans;
    }

    /**
     * Gets the {@link Timer} for close requests.
     *
     * @return the {@link Timer} for close requests.
     */
    public Timer getCloses() {
        return closes;
    }
}
