package com.datasift.dropwizard.hbase.scanner

import com.yammer.metrics.scala.Instrumented
import com.stumbleupon.async.Deferred
import java.util.ArrayList
import org.hbase.async.KeyValue

/** Instruments a [[com.datasift.dropwizard.hbase.scanner.RowScanner]] with [[com.yammer.metrics]] */
class InstrumentedRowScanner private[hbase](underlying: RowScanner)
  extends RowScanner
  with Instrumented {

  private val closes = metrics.timer("close", "scanner")
  private val scans = metrics.timer("scans", "scanner")

  /** closes this Scanner */
  def close(): Deferred[AnyRef] = closes.time {
    underlying.close()
  }

  /** scans the next batch of rows
   *
   * @return next batch of rows that were scanned
   */
  def nextRows(): Deferred[ArrayList[ArrayList[KeyValue]]] = scans.time {
    underlying.nextRows()
  }

  /** scans the next batch of rows
   *
   * @param rows maximum number of rows to retrieve in the batch
   * @return next batch of rows that were scanned
   */
  def nextRows(rows: Int): Deferred[ArrayList[ArrayList[KeyValue]]] = scans.time {
    underlying.nextRows(rows)
  }
}
