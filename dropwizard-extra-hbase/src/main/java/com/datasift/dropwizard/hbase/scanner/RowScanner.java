package com.datasift.dropwizard.hbase.scanner;

import com.stumbleupon.async.Deferred;
import org.hbase.async.KeyValue;

import java.nio.charset.Charset;
import java.util.ArrayList;

/**
 * Client for scanning over a selection of rows.
 * <p>
 * To obtain an instance of a {@link RowScanner}, call {@link
 * com.datasift.dropwizard.hbase.HBaseClient#scan(byte[])}.
 * <p>
 * All implementations are wrapper proxies around {@link org.hbase.async.Scanner} providing
 * additional functionality.
 */
public interface RowScanner {

    /**
     * Set the first key in the range to scan.
     *
     * @param key the first key to scan from (inclusive).
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setStartKey(byte[])
     */
    public RowScanner setStartKey(byte[] key);

    /**
     * Set the first key in the range to scan.
     *
     * @param key the first key to scan from (inclusive).
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setStartKey(String)
     */
    public RowScanner setStartKey(String key);

    /**
     * Set the end key in the range to scan.
     *
     * @param key the end key to scan until (exclusive).
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setStopKey(byte[])
     */
    public RowScanner setStopKey(byte[] key);

    /**
     * Set the end key in the range to scan.
     *
     * @param key the end key to scan until (exclusive).
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setStopKey(byte[])
     */
    public RowScanner setStopKey(String key);

    /**
     * Set the family to scan.
     *
     * @param family the family to scan.
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setFamily(byte[])
     */
    public RowScanner setFamily(byte[] family);

    /**
     * Set the family to scan.
     *
     * @param family the family to scan.
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setFamily(String)
     */
    public RowScanner setFamily(String family);

    /**
     * Set the qualifier to select from cells
     *
     * @param qualifier the family to select from cells.
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setQualifier(byte[])
     */
    public RowScanner setQualifier(byte[] qualifier);

    /**
     * Set the qualifiers to select from cells
     *
     * @param qualifiers the family to select from cells.
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setQualifiers(byte[][])
     */
    public RowScanner setQualifiers(byte[][] qualifiers);


    /**
     * Set the qualifier to select from cells
     *
     * @param qualifier the family to select from cells.
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setQualifier(String)
     */
    public RowScanner setQualifier(String qualifier);

    /**
     * Set a regular expression to filter keys being scanned.
     *
     * @param regexp a regular expression to filter keys with.
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setKeyRegexp(String)
     */
    public RowScanner setKeyRegexp(String regexp);

    /**
     * Set a regular expression to filter keys being scanned.
     *
     * @param regexp a regular expression to filter keys with.
     * @param charset the charset to decode the keys as.
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setKeyRegexp(String)
     */
    public RowScanner setKeyRegexp(String regexp, Charset charset);

    /**
     * Set whether to use the server-side block cache during the scan.
     *
     * @param populateBlockcache whether to use the server-side block cache.
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setServerBlockCache(boolean)
     */
    public RowScanner setServerBlockCache(boolean populateBlockcache);

    /**
     * Set the maximum number of rows to fetch in each batch.
     *
     * @param maxRows the maximum number of rows to fetch in each batch.
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setMaxNumRows(int)
     */
    public RowScanner setMaxNumRows(int maxRows);

    /**
     * Set the maximum number of {@link KeyValue}s to fetch in each batch.
     *
     * @param maxKeyValues the maximum number of {@link KeyValue}s to fetch in each batch.
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     */
    public RowScanner setMaxNumKeyValues(int maxKeyValues);

    /**
     * Sets the minimum timestamp of the cells to yield.
     *
     * @param timestamp the minimum timestamp of the cells to yield.
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setMinTimestamp(long)
     */
    public RowScanner setMinTimestamp(long timestamp);

    /**
     * Gets the minimum timestamp of the cells to yield.
     *
     * @return the minimum timestamp of the cells to yield.
     *
     * @see org.hbase.async.Scanner#getMinTimestamp()
     */
    public long getMinTimestamp();

    /**
     * Sets the maximum timestamp of the cells to yield.
     *
     * @param timestamp the maximum timestamp of the cells to yield.
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setMaxTimestamp(long)
     */
    public RowScanner setMaxTimestamp(long timestamp);

    /**
     * Gets the maximum timestamp of the cells to yield.
     *
     * @return the maximum timestamp of the cells to yield.
     *
     * @see org.hbase.async.Scanner#getMaxTimestamp()
     */
    public long getMaxTimestamp();

    /**
     * Sets the timerange of the cells to yield.
     *
     * @param minTimestamp the minimum timestamp of the cells to yield.
     * @param maxTimestamp the maximum timestamp of the cells to yield.
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setMinTimestamp(long)
     */
    public RowScanner setTimeRange(long minTimestamp, long maxTimestamp);

    /**
     * Get the key of the current row being scanned.
     *
     * @return the key of the current row.
     *
     * @see org.hbase.async.Scanner#getCurrentKey()
     */
    public byte[] getCurrentKey();

    /**
     * Closes this Scanner
     *
     * @return a Deferred indicating when the close operation has completed.
     *
     * @see org.hbase.async.Scanner#close()
     */
    public Deferred<Object> close();

    /**
     * Scans the next batch of rows
     *
     * @return next batch of rows that were scanned.
     *
     * @see org.hbase.async.Scanner#nextRows()
     */
    public Deferred<ArrayList<ArrayList<KeyValue>>> nextRows();

    /**
     * Scans the next batch of rows
     *
     * @param rows maximum number of rows to retrieve in the batch.
     *
     * @return next batch of rows that were scanned.
     *
     * @see org.hbase.async.Scanner#nextRows(int)
     */
    public Deferred<ArrayList<ArrayList<KeyValue>>> nextRows(int rows);
}
