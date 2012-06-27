package com.datasift.dropwizard.hbase.scanner

import com.stumbleupon.async.Deferred
import java.util.ArrayList
import org.hbase.async.{KeyValue, Scanner}
import com.datasift.dropwizard.hbase.config.ScannerConfiguration

/** Iterates over a selection of rows on the server-side
 *
 * You may not create an instance of this class directly. Instead, use the factory
 * provided by [[com.datasift.dropwizard.hbase.HBase.newScanner( )]]
 *
 * @param underlying underlying [[org.hbase.async.Scanner]] to use for iteration
 */
class RowScannerProxy(underlying: Scanner, config: ScannerConfiguration)
  extends RowScanner {

  // initialize the Scanner
  config.family foreach { underlying.setFamily(_) }
  config.qualifier foreach { underlying.setQualifier(_) }
  underlying.setMaxTimestamp(config.maxTimestamp)
  underlying.setMinTimestamp(config.minTimestamp)
  config.keyRegexp foreach { underlying.setKeyRegexp(_) }
  underlying.setMaxNumKeyValues(config.maxKeyValues)
  underlying.setMaxNumRows(config.maxRows)
  underlying.setServerBlockCache(config.enableBlockCache)
  config.startKey foreach { underlying.setStartKey(_) }
  config.stopKey foreach { underlying.setStopKey(_) }


  /** closes this Scanner */
  def close(): Deferred[AnyRef] = {
    underlying.close()
  }

  /** scans the next batch of rows
   *
   * @return next batch of rows that were scanned
   */
  def nextRows(): Deferred[ArrayList[ArrayList[KeyValue]]] = {
    underlying.nextRows()
  }

  /** scans the next batch of rows
   *
   * @param rows maximum number of rows to retrieve in the batch
   * @return next batch of rows that were scanned
   */
  def nextRows(rows: Int): Deferred[ArrayList[ArrayList[KeyValue]]] = {
    underlying.nextRows(rows)
  }

  override def toString = underlying.toString
}
