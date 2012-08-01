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

    private RowScanner scanner;
    private HBaseInstrumentation metrics;

    public InstrumentedRowScanner(RowScanner scanner,
                                  HBaseInstrumentation metrics) {
        this.scanner = scanner;
        this.metrics = metrics;
    }

    public byte[] getCurrentKey() {
        return scanner.getCurrentKey();
    }

    public void setStartKey(byte[] start_key) {
        scanner.setStartKey(start_key);
    }

    public void setStartKey(String start_key) {
        scanner.setStartKey(start_key);
    }

    public void setStopKey(byte[] stop_key) {
        scanner.setStopKey(stop_key);
    }

    public void setStopKey(String stop_key) {
        scanner.setStopKey(stop_key);
    }

    public void setFamily(byte[] family) {
        scanner.setFamily(family);
    }

    public void setFamily(String family) {
        scanner.setFamily(family);
    }

    public void setQualifier(byte[] qualifier) {
        scanner.setQualifier(qualifier);
    }

    public void setQualifier(String qualifier) {
        scanner.setQualifier(qualifier);
    }

    public void setKeyRegexp(String regexp) {
        scanner.setKeyRegexp(regexp);
    }

    public void setKeyRegexp(String regexp, Charset charset) {
        scanner.setKeyRegexp(regexp, charset);
    }

    public void setServerBlockCache(boolean populate_blockcache) {
        scanner.setServerBlockCache(populate_blockcache);
    }

    public void setMaxNumRows(int max_num_rows) {
        scanner.setMaxNumRows(max_num_rows);
    }

    public void setMaxNumKeyValues(int max_num_kvs) {
        scanner.setMaxNumKeyValues(max_num_kvs);
    }

    public void setMinTimestamp(long timestamp) {
        scanner.setMinTimestamp(timestamp);
    }

    public long getMinTimestamp() {
        return scanner.getMinTimestamp();
    }

    public void setMaxTimestamp(long timestamp) {
        scanner.setMaxTimestamp(timestamp);
    }

    public long getMaxTimestamp() {
        return scanner.getMaxTimestamp();
    }

    public void setTimeRange(long min_timestamp, long max_timestamp) {
        scanner.setTimeRange(min_timestamp, max_timestamp);
    }

    public Deferred<Object> close() {
        TimerContext ctx = metrics.getCloses().time();
        return scanner.close().addBoth(new TimerStoppingCallback<Object>(ctx));
    }

    public Deferred<ArrayList<ArrayList<KeyValue>>> nextRows() {
        TimerContext ctx = metrics.getScans().time();
        return scanner.nextRows().addBoth(new TimerStoppingCallback<ArrayList<ArrayList<KeyValue>>>(ctx));
    }

    public Deferred<ArrayList<ArrayList<KeyValue>>> nextRows(int rows) {
        TimerContext ctx = metrics.getScans().time();
        return scanner.nextRows(rows).addBoth(new TimerStoppingCallback<ArrayList<ArrayList<KeyValue>>>(ctx));
    }
}
