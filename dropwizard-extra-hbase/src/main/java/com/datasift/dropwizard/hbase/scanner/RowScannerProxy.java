package com.datasift.dropwizard.hbase.scanner;

import com.stumbleupon.async.Deferred;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import java.nio.charset.Charset;
import java.util.ArrayList;

/**
 * Client for scanning over a selection of rows.
 * <p/>
 * To obtain an instance of a {@link RowScanner}, call
 * {@link com.datasift.dropwizard.hbase.HBaseClient#scan(byte[])}.
 * <p/>
 * This implementation is a proxy for a {@link org.hbase.async.Scanner}.
 */
public class RowScannerProxy implements RowScanner {

    private final Scanner scanner;

    /**
     * Creates a new {@link RowScannerProxy} for the given {@link Scanner}.
     *
     * @param scanner the underlying {@link Scanner} to wrap
     */
    public RowScannerProxy(final Scanner scanner) {
        this.scanner = scanner;
    }

    /**
     * Set the first key in the range to scan.
     *
     * @see org.hbase.async.Scanner#setStartKey(byte[])
     * @param key the first key to scan from (inclusive)
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setStartKey(final byte[] key) {
        scanner.setStartKey(key);
        return this;
    }

    /**
     * Set the first key in the range to scan.
     *
     * @see org.hbase.async.Scanner#setStartKey(String)
     * @param key the first key to scan from (inclusive)
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setStartKey(final String key) {
        scanner.setStartKey(key);
        return this;
    }
    /**
     * Set the end key in the range to scan.
     *
     * @see org.hbase.async.Scanner#setStopKey(byte[])
     * @param key the end key to scan until (exclusive)
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setStopKey(final byte[] key) {
        scanner.setStopKey(key);
        return this;
    }

    /**
     * Set the end key in the range to scan.
     *
     * @see org.hbase.async.Scanner#setStopKey(byte[])
     * @param key the end key to scan until (exclusive)
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setStopKey(final String key) {
        scanner.setStopKey(key);
        return this;
    }

    /**
     * Set the family to scan.
     *
     * @see org.hbase.async.Scanner#setFamily(byte[])
     * @param family the family to scan
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setFamily(final byte[] family) {
        scanner.setFamily(family);
        return this;
    }

    /**
     * Set the family to scan.
     *
     * @see org.hbase.async.Scanner#setFamily(String)
     * @param family the family to scan
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setFamily(final String family) {
        scanner.setFamily(family);
        return this;
    }

    /**
     * Set the qualifier to select from cells
     *
     * @see org.hbase.async.Scanner#setQualifier(byte[])
     * @param qualifier the family to select from cells
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setQualifier(final byte[] qualifier) {
        scanner.setQualifier(qualifier);
        return this;
    }

    /**
     * Set the qualifier to select from cells
     *
     * @see org.hbase.async.Scanner#setQualifier(String)
     * @param qualifier the family to select from cells
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setQualifier(final String qualifier) {
        scanner.setQualifier(qualifier);
        return this;
    }

    /**
     * Set a regular expression to filter keys being scanned.
     *
     * @see org.hbase.async.Scanner#setKeyRegexp(String)
     * @param regexp a regular expression to filter keys with
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setKeyRegexp(final String regexp) {
        scanner.setKeyRegexp(regexp);
        return this;
    }

    /**
     * Set a regular expression to filter keys being scanned.
     *
     * @see org.hbase.async.Scanner#setKeyRegexp(String)
     * @param regexp a regular expression to filter keys with
     * @param charset the charset to decode the keys as
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setKeyRegexp(final String regexp, final Charset charset) {
        scanner.setKeyRegexp(regexp, charset);
        return this;
    }

    /**
     * Set whether to use the server-side block cache during the scan.
     *
     * @see org.hbase.async.Scanner#setServerBlockCache(boolean)
     * @param populateBlockcache whether to use the server-side block cache
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setServerBlockCache(final boolean populateBlockcache) {
        scanner.setServerBlockCache(populateBlockcache);
        return this;
    }

    /**
     * Set the maximum number of rows to fetch in each batch.
     *
     * @see org.hbase.async.Scanner#setMaxNumRows(int)
     * @param maxRows the maximum number of rows to fetch in each batch
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setMaxNumRows(final int maxRows) {
        scanner.setMaxNumRows(maxRows);
        return this;
    }

    /**
     * Set the maximum number of {@link KeyValue}s to fetch in each batch.
     *
     * @see org.hbase.async.Scanner#setMaxNumKeyValues(int)
     * @param maxKeyValues the maximum number of {@link KeyValue}s to fetch in
     *                    each batch
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setMaxNumKeyValues(final int maxKeyValues) {
        scanner.setMaxNumKeyValues(maxKeyValues);
        return this;
    }

    /**
     * Sets the minimum timestamp of the cells to yield.
     *
     * @see org.hbase.async.Scanner#setMinTimestamp(long)
     * @param timestamp the minimum timestamp of the cells to yield
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setMinTimestamp(final long timestamp) {
        scanner.setMinTimestamp(timestamp);
        return this;
    }

    /**
     * Gets the minimum timestamp of the cells to yield.
     *
     * @see org.hbase.async.Scanner#getMinTimestamp()
     * @return the minimum timestamp of the cells to yield
     */
    public long getMinTimestamp() {
        return scanner.getMinTimestamp();
    }

    /**
     * Sets the maximum timestamp of the cells to yield.
     *
     * @see org.hbase.async.Scanner#setMaxTimestamp(long)
     * @param timestamp the maximum timestamp of the cells to yield
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setMaxTimestamp(final long timestamp) {
        scanner.setMaxTimestamp(timestamp);
        return this;
    }

    /**
     * Gets the maximum timestamp of the cells to yield.
     *
     * @see org.hbase.async.Scanner#getMaxTimestamp()
     * @return the maximum timestamp of the cells to yield
     */
    public long getMaxTimestamp() {
        return scanner.getMaxTimestamp();
    }

    /**
     * Sets the timerange of the cells to yield.
     *
     * @see org.hbase.async.Scanner#setMinTimestamp(long)
     * @param minTimestamp the minimum timestamp of the cells to yield
     * @param maxTimestamp the maximum timestamp of the cells to yield
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setTimeRange(final long minTimestamp, final long maxTimestamp) {
        scanner.setTimeRange(minTimestamp, maxTimestamp);
        return this;
    }

    /**
     * Get the key of the current row being scanned.
     *
     * @see org.hbase.async.Scanner#getCurrentKey()
     * @return the key of the current row
     */
    public byte[] getCurrentKey() {
        return scanner.getCurrentKey();
    }

    /**
     * Closes this Scanner
     *
     * @see org.hbase.async.Scanner#close()
     * @return a Deferred indicating when the close operation has completed
     */
    public Deferred<Object> close() {
        return scanner.close();
    }

    /**
     * Scans the next batch of rows
     *
     * @see org.hbase.async.Scanner#nextRows()
     * @return next batch of rows that were scanned
     */
    public Deferred<ArrayList<ArrayList<KeyValue>>> nextRows() {
        return scanner.nextRows();
    }

    /**
     * Scans the next batch of rows
     *
     * @see org.hbase.async.Scanner#nextRows(int)
     * @param rows maximum number of rows to retrieve in the batch
     * @return next batch of rows that were scanned
     */
    public Deferred<ArrayList<ArrayList<KeyValue>>> nextRows(final int rows) {
        return scanner.nextRows(rows);
    }
}
