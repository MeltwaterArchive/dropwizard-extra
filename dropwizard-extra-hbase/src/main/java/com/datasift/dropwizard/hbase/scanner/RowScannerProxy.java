package com.datasift.dropwizard.hbase.scanner;

import com.stumbleupon.async.Deferred;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import java.nio.charset.Charset;
import java.util.ArrayList;

/**
 * Proxies the {@link RowScanner} API to a {@link org.hbase.async.Scanner}.
 */
public class RowScannerProxy implements RowScanner {

    private final Scanner scanner;

    public RowScannerProxy(final Scanner scanner) {
        this.scanner = scanner;
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
        return scanner.close();
    }

    public Deferred<ArrayList<ArrayList<KeyValue>>> nextRows() {
        return scanner.nextRows();
    }

    public Deferred<ArrayList<ArrayList<KeyValue>>> nextRows(final int rows) {
        return scanner.nextRows(rows);
    }
}
