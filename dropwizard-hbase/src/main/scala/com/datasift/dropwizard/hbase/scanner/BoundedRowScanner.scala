package com.datasift.dropwizard.hbase.scanner

import java.util.concurrent.Semaphore
import com.stumbleupon.async.Deferred
import java.util.ArrayList
import org.hbase.async.{KeyValue, Scanner}
import com.datasift.dropwizard.hbase.PermitReleasingCallback

/**Iterates over a selection of rows with an upper-bounds on the number of concurrent requests
 *
 * You may not create an instance of this class directly. Instead, use the factory
 * provided by [[com.datasift.dropwizard.hbase.HBase.newScanner( )]]
 *
 * The [[java.util.concurrent.Semaphore]] bounding this RowScanner is provided
 * by the [[com.datasift.dropwizard.hbase.BoundedHBase]] object that creates it.
 *
 * @param underlying underlying [[org.hbase.async.Scanner]] to use for iteration
 * @param semaphore semaphore that limits the number of concurrent requests
 */
class BoundedRowScanner private[hbase](underlying: RowScanner, semaphore: Semaphore)
  extends RowScanner {

  /**@see com.datasift.dropwizard.hbase.RowScanner.close() */
  def close(): Deferred[AnyRef] = {
    semaphore.acquire()
    underlying.close().addBoth(new PermitReleasingCallback[AnyRef](semaphore))
  }

  /**@see com.datasift.dropwizard.hbase.RowScanner.nextRows() */
  def nextRows(): Deferred[ArrayList[ArrayList[KeyValue]]] = {
    semaphore.acquire()
    underlying
      .nextRows()
      .addBoth(new PermitReleasingCallback[ArrayList[ArrayList[KeyValue]]](semaphore))
  }

  /**@see com.datasift.dropwizard.hbase.RowScanner.nextRows() */
  def nextRows(rows: Int): Deferred[ArrayList[ArrayList[KeyValue]]] = {
    semaphore.acquire()
    underlying
      .nextRows(rows)
      .addBoth(new PermitReleasingCallback[ArrayList[ArrayList[KeyValue]]](semaphore))
  }
}
