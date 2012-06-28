package com.datasift.dropwizard.hbase.scanner

import com.stumbleupon.async.Deferred
import java.util.ArrayList
import org.hbase.async.KeyValue

trait RowScanner {

  /** closes this Scanner */
  def close(): Deferred[AnyRef]

  /** scans the next batch of rows
   *
   * @return next batch of rows that were scanned
   */
  def nextRows(): Deferred[ArrayList[ArrayList[KeyValue]]]

  /** scans the next batch of rows
   *
   * @param rows maximum number of rows to retrieve in the batch
   * @return next batch of rows that were scanned
   */
  def nextRows(rows: Int): Deferred[ArrayList[ArrayList[KeyValue]]]
}
