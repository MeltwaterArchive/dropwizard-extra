package com.datasift.dropwizard.hbase.scanner;

import com.datasift.dropwizard.hbase.metrics.HBaseInstrumentation;
import com.datasift.dropwizard.hbase.util.TimerStoppingCallback;
import com.stumbleupon.async.Deferred;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.hbase.async.KeyValue;

import java.nio.charset.Charset;
import java.util.ArrayList;

/**
 * A {@link RowScanner} that is instrumented with {@link com.yammer.metrics.Metrics}
 */
public class InstrumentedRowScanner implements RowScanner {

    private final RowScanner scanner;
    private final HBaseInstrumentation metrics;

    public InstrumentedRowScanner(final RowScanner scanner,
                                  final HBaseInstrumentation metrics) {
        this.scanner = scanner;
        this.metrics = metrics;
    }

    public byte[] getCurrentKey() {
        return scanner.getCurrentKey();
    }

    public void setStartKey(final byte[] start_key) {
        scanner.setStartKey(start_key);
    }

    public void setStartKey(final String start_key) {
        scanner.setStartKey(start_key);
    }

    public void setStopKey(final byte[] stop_key) {
        scanner.setStopKey(stop_key);
    }

    public void setStopKey(final String stop_key) {
        scanner.setStopKey(stop_key);
    }

    public void setFamily(final byte[] family) {
        scanner.setFamily(family);
    }

    public void setFamily(final String family) {
        scanner.setFamily(family);
    }

    public void setQualifier(final byte[] qualifier) {
        scanner.setQualifier(qualifier);
    }

    public void setQualifier(final String qualifier) {
        scanner.setQualifier(qualifier);
    }

    public void setKeyRegexp(final String regexp) {
        scanner.setKeyRegexp(regexp);
    }

    public void setKeyRegexp(final String regexp, final Charset charset) {
        scanner.setKeyRegexp(regexp, charset);
    }

    public void setServerBlockCache(final boolean populate_blockcache) {
        scanner.setServerBlockCache(populate_blockcache);
    }

    public void setMaxNumRows(final int max_num_rows) {
        scanner.setMaxNumRows(max_num_rows);
    }

    public void setMaxNumKeyValues(final int max_num_kvs) {
        scanner.setMaxNumKeyValues(max_num_kvs);
    }

    public void setMinTimestamp(final long timestamp) {
        scanner.setMinTimestamp(timestamp);
    }

    public long getMinTimestamp() {
        return scanner.getMinTimestamp();
    }

    public void setMaxTimestamp(final long timestamp) {
        scanner.setMaxTimestamp(timestamp);
    }

    public long getMaxTimestamp() {
        return scanner.getMaxTimestamp();
    }

    public void setTimeRange(final long min_timestamp,
                             final long max_timestamp) {
        scanner.setTimeRange(min_timestamp, max_timestamp);
    }

    public Deferred<Object> close() {
        final TimerContext ctx = metrics.getCloses().time();
        return scanner.close().addBoth(new TimerStoppingCallback<Object>(ctx));
    }

    public Deferred<ArrayList<ArrayList<KeyValue>>> nextRows() {
        final TimerContext ctx = metrics.getScans().time();
        return scanner.nextRows()
                .addBoth(new TimerStoppingCallback<ArrayList<ArrayList<KeyValue>>>(ctx));
    }

    public Deferred<ArrayList<ArrayList<KeyValue>>> nextRows(final int rows) {
        final TimerContext ctx = metrics.getScans().time();
        return scanner.nextRows(rows)
                .addBoth(new TimerStoppingCallback<ArrayList<ArrayList<KeyValue>>>(ctx));
    }
}
