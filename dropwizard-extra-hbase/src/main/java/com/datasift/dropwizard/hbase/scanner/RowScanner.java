package com.datasift.dropwizard.hbase.scanner;

import com.stumbleupon.async.Deferred;
import org.hbase.async.KeyValue;

import java.nio.charset.Charset;
import java.util.ArrayList;

/**
 * Client for scanning over a selection of rows.
 *
 * To obtain an instance of a {@link RowScanner},
 * call {@link com.datasift.dropwizard.hbase.HBaseClient#newScanner(byte[])}.
 *
 * All implementations are wrapper proxies around {@link org.hbase.async.Scanner}
 * providing additional functionality.
 */
public interface RowScanner {

    /**
     * Get the key of the current row being scanned.
     *
     * @see org.hbase.async.Scanner#getCurrentKey()
     * @return the key of the current row
     */
    public byte[] getCurrentKey();

    /**
     * Set the first key in the range to scan.
     *
     * @see org.hbase.async.Scanner#setStartKey(byte[])
     * @param start_key the first key to scan from (inclusive)
     */
    public void setStartKey(byte[] start_key);

    /**
     * Set the first key in the range to scan.
     *
     * @see org.hbase.async.Scanner#setStartKey(String)
     * @param start_key the first key to scan from (inclusive)
     */
    public void setStartKey(String start_key);

    /**
     * Set the end key in the range to scan.
     *
     * @see org.hbase.async.Scanner#setStopKey(byte[])
     * @param stop_key the end key to scan until (exclusive)
     */
    public void setStopKey(byte[] stop_key);

    /**
     * Set the end key in the range to scan.
     *
     * @see org.hbase.async.Scanner#setStopKey(byte[])
     * @param stop_key the end key to scan until (exclusive)
     */
    public void setStopKey(String stop_key);

    /**
     * Set the family to scan.
     *
     * @see org.hbase.async.Scanner#setFamily(byte[])
     * @param family the family to scan
     */
    public void setFamily(byte[] family);

    /**
     * Set the family to scan.
     *
     * @see org.hbase.async.Scanner#setFamily(String)
     * @param family the family to scan
     */
    public void setFamily(String family);

    /**
     * Set the qualifier to select from cells
     *
     * @see org.hbase.async.Scanner#setQualifier(byte[])
     * @param qualifier the family to select from cells
     */
    public void setQualifier(byte[] qualifier);

    /**
     * Set the qualifier to select from cells
     *
     * @see org.hbase.async.Scanner#setQualifier(String)
     * @param qualifier the family to select from cells
     */
    public void setQualifier(String qualifier);

    /**
     * Set a regular expression to filter keys being scanned.
     *
     * @see org.hbase.async.Scanner#setKeyRegexp(String)
     * @param regexp a regular expression to filter keys with
     */
    public void setKeyRegexp(String regexp);

    /**
     * Set a regular expression to filter keys being scanned.
     *
     * @see org.hbase.async.Scanner#setKeyRegexp(String)
     * @param regexp a regular expression to filter keys with
     * @param charset the charset to decode the keys as
     */
    public void setKeyRegexp(String regexp, Charset charset);

    /**
     * Set whether to use the server-side block cache during the scan.
     *
     * @see org.hbase.async.Scanner#setServerBlockCache(boolean)
     * @param populate_blockcache whether to use the server-side block cache
     */
    public void setServerBlockCache(boolean populate_blockcache);

    /**
     * Set the maximum number of rows to fetch in each batch.
     *
     * @see org.hbase.async.Scanner#setMaxNumRows(int)
     * @param max_num_rows the maximum number of rows to fetch in each batch
     */
    public void setMaxNumRows(int max_num_rows);

    /**
     * Set the maximum number of {@link KeyValue}s to fetch in each batch.
     *
     * @see org.hbase.async.Scanner#setMaxNumKeyValues(int)
     * @param max_num_kvs the maximum number of {@link KeyValue}s to fetch in
     *                    each batch
     */
    public void setMaxNumKeyValues(int max_num_kvs);

    /**
     * Sets the minimum timestamp of the cells to yield.
     *
     * @see org.hbase.async.Scanner#setMinTimestamp(long)
     * @param timestamp the minimum timestamp of the cells to yield
     */
    public void setMinTimestamp(long timestamp);

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
     */
    public void setMaxTimestamp(long timestamp);

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
     * @param min_timestamp the minimum timestamp of the cells to yield
     * @param max_timestamp the maximum timestamp of the cells to yield
     */
    public void setTimeRange(long min_timestamp, long max_timestamp);

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
