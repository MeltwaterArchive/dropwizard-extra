package com.datasift.dropwizard.hbase.scanner;

import com.stumbleupon.async.Deferred;
import org.hbase.async.KeyValue;

import java.nio.charset.Charset;
import java.util.ArrayList;

/**
 * Client for scanning over a selection of rows.
 * <p/>
 * To obtain an instance of a {@link RowScanner}, call
 * {@link com.datasift.dropwizard.hbase.HBaseClient#scan(byte[])}.
 * <p/>
 * All implementations are wrapper proxies around {@link org.hbase.async.Scanner} providing
 * additional functionality.
 */
public interface RowScanner {

    /**
     * Set the first key in the range to scan.
     *
     * @see org.hbase.async.Scanner#setStartKey(byte[])
     * @param key the first key to scan from (inclusive)
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setStartKey(byte[] key);

    /**
     * Set the first key in the range to scan.
     *
     * @see org.hbase.async.Scanner#setStartKey(String)
     * @param key the first key to scan from (inclusive)
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setStartKey(String key);

    /**
     * Set the end key in the range to scan.
     *
     * @see org.hbase.async.Scanner#setStopKey(byte[])
     * @param key the end key to scan until (exclusive)
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setStopKey(byte[] key);

    /**
     * Set the end key in the range to scan.
     *
     * @see org.hbase.async.Scanner#setStopKey(byte[])
     * @param key the end key to scan until (exclusive)
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setStopKey(String key);

    /**
     * Set the family to scan.
     *
     * @see org.hbase.async.Scanner#setFamily(byte[])
     * @param family the family to scan
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setFamily(byte[] family);

    /**
     * Set the family to scan.
     *
     * @see org.hbase.async.Scanner#setFamily(String)
     * @param family the family to scan
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setFamily(String family);

    /**
     * Set the qualifier to select from cells
     *
     * @see org.hbase.async.Scanner#setQualifier(byte[])
     * @param qualifier the family to select from cells
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setQualifier(byte[] qualifier);

    /**
     * Set the qualifier to select from cells
     *
     * @see org.hbase.async.Scanner#setQualifier(String)
     * @param qualifier the family to select from cells
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setQualifier(String qualifier);

    /**
     * Set a regular expression to filter keys being scanned.
     *
     * @see org.hbase.async.Scanner#setKeyRegexp(String)
     * @param regexp a regular expression to filter keys with
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setKeyRegexp(String regexp);

    /**
     * Set a regular expression to filter keys being scanned.
     *
     * @see org.hbase.async.Scanner#setKeyRegexp(String)
     * @param regexp a regular expression to filter keys with
     * @param charset the charset to decode the keys as
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setKeyRegexp(String regexp, Charset charset);

    /**
     * Set whether to use the server-side block cache during the scan.
     *
     * @see org.hbase.async.Scanner#setServerBlockCache(boolean)
     * @param populateBlockcache whether to use the server-side block cache
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setServerBlockCache(boolean populateBlockcache);

    /**
     * Set the maximum number of rows to fetch in each batch.
     *
     * @see org.hbase.async.Scanner#setMaxNumRows(int)
     * @param maxRows the maximum number of rows to fetch in each batch
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setMaxNumRows(int maxRows);

    /**
     * Set the maximum number of {@link KeyValue}s to fetch in each batch.
     *
     * @see org.hbase.async.Scanner#setMaxNumKeyValues(int)
     * @param maxKeyValues the maximum number of {@link KeyValue}s to fetch in
     *                    each batch
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setMaxNumKeyValues(int maxKeyValues);

    /**
     * Sets the minimum timestamp of the cells to yield.
     *
     * @see org.hbase.async.Scanner#setMinTimestamp(long)
     * @param timestamp the minimum timestamp of the cells to yield
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setMinTimestamp(long timestamp);

    /**
     * Gets the minimum timestamp of the cells to yield.
     *
     * @see org.hbase.async.Scanner#getMinTimestamp()
     * @return the minimum timestamp of the cells to yield
     */
    public long getMinTimestamp();

    /**
     * Sets the maximum timestamp of the cells to yield.
     *
     * @see org.hbase.async.Scanner#setMaxTimestamp(long)
     * @param timestamp the maximum timestamp of the cells to yield
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setMaxTimestamp(long timestamp);

    /**
     * Gets the maximum timestamp of the cells to yield.
     *
     * @see org.hbase.async.Scanner#getMaxTimestamp()
     * @return the maximum timestamp of the cells to yield
     */
    public long getMaxTimestamp();

    /**
     * Sets the timerange of the cells to yield.
     *
     * @see org.hbase.async.Scanner#setMinTimestamp(long)
     * @param minTimestamp the minimum timestamp of the cells to yield
     * @param maxTimestamp the maximum timestamp of the cells to yield
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setTimeRange(long minTimestamp, long maxTimestamp);

    /**
     * Get the key of the current row being scanned.
     *
     * @see org.hbase.async.Scanner#getCurrentKey()
     * @return the key of the current row
     */
    public byte[] getCurrentKey();

    /**
     * Closes this Scanner
     *
     * @see org.hbase.async.Scanner#close()
     * @return a Deferred indicating when the close operation has completed
     */
    public Deferred<Object> close();

    /**
     * Scans the next batch of rows
     *
     * @see org.hbase.async.Scanner#nextRows()
     * @return next batch of rows that were scanned
     */
    public Deferred<ArrayList<ArrayList<KeyValue>>> nextRows();

    /**
     * Scans the next batch of rows
     *
     * @see org.hbase.async.Scanner#nextRows(int)
     * @param rows maximum number of rows to retrieve in the batch
     * @return next batch of rows that were scanned
     */
    public Deferred<ArrayList<ArrayList<KeyValue>>> nextRows(int rows);
}
