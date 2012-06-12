package com.datasift.dropwizard.hbase

import java.util.concurrent.Semaphore
import com.stumbleupon.async.Deferred
import java.util.ArrayList
import org.hbase.async._
import scanner.{RowScanner, BoundedRowScanner}
import com.datasift.dropwizard.config.ScannerConfiguration
import org.jboss.netty.util.Timer

/** Client for interacting with an HBase cluster with an upper-bounds on concurrent requests
 *
 * You may not create an instance of this class directly. Instead, use the factory
 * provided by [[com.datasift.dropwizard.hbase.HBase.apply()]]
 *
 * This client places an upper-bounds on the number of concurrent requests awaiting
 * completion. When this limit is reached, subsequent requests will block until
 * existing requests have completed.
 *
 * This behaviour is particularly useful for throttling high-throughput bulk
 * insert applications where HBase is the bottle-neck. Without backing-off, such
 * an application may run out of memory. By setting the max requests to a
 * sufficiently high limit, but low enough so that it can be reached without
 * running out of memory, such applications can organically throttle and back-off
 * their writes.
 *
 * Book-keeping of in-flight requests is done using a [[java.util.concurrent.Semaphore]]
 * which is configured as "non-fair" to reduce its impact on request throughput.
 *
 * @param underlying underlying client for interacting with the HBase cluster
 * @param maxRequests maximum number of concurrent requests awaiting completion
 *                    before blocking new requests
 */
class BoundedHBase private[hbase] (underlying: HBase, maxRequests: Int) extends HBase {

  private val semaphore = new Semaphore(maxRequests)

  /** maximum time in milliseconds for which edits can be buffered */
  val flushInterval: Short = underlying.flushInterval

  /** capacity of the increment buffer */
  val incrementBufferSize: Int = underlying.incrementBufferSize

  /** @see com.datasift.dropwizard.hbase.HBase.create() */
  def create(edit: PutRequest): Deferred[Boolean] = withPermit {
    underlying.create(edit)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.increment() */
  def increment(request: AtomicIncrementRequest): Deferred[Long] = withPermit {
    underlying.increment(request)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.increment() */
  def increment(request: AtomicIncrementRequest, durable: Boolean): Deferred[Long] = withPermit {
    underlying.increment(request, durable)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.compareAndSet() */
  def compareAndSet(edit: PutRequest, expected: Array[Byte]): Deferred[Boolean] = withPermit {
    underlying.compareAndSet(edit, expected)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.compareAndSet() */
  def compareAndSet(edit: PutRequest, expected: String): Deferred[Boolean] = withPermit {
    underlying.compareAndSet(edit, expected)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.delete() */
  def delete(request: DeleteRequest): Deferred[AnyRef] = withPermit {
    underlying.delete(request)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.ensureTableExists() */
  def ensureTableExists(table: Array[Byte]): Deferred[AnyRef] = withPermit {
    underlying.ensureTableExists(table)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.ensureTableExists() */
  def ensureTableExists(table: String): Deferred[AnyRef] = withPermit {
    underlying.ensureTableExists(table)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.ensureTableFamilyExists() */
  def ensureTableFamilyExists(table: Array[Byte], family: Array[Byte]): Deferred[AnyRef] = withPermit {
    underlying.ensureTableFamilyExists(table, family)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.ensureTableFamilyExists() */
  def ensureTableFamilyExists(table: String, family: String): Deferred[AnyRef] = withPermit {
    underlying.ensureTableFamilyExists(table, family)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.flush() */
  def flush(): Deferred[AnyRef] = withPermit {
    underlying.flush()
  }

  /** @see com.datasift.dropwizard.hbase.HBase.get() */
  def get(request: GetRequest): Deferred[ArrayList[KeyValue]] = withPermit {
    underlying.get(request)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.lockRow() */
  def lockRow(request: RowLockRequest): Deferred[RowLock] = withPermit {
    underlying.lockRow(request)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.put() */
  def put(request: PutRequest): Deferred[AnyRef] = withPermit {
    underlying.put(request)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.shutdown() */
  def shutdown(): Deferred[AnyRef] = withPermit {
    underlying.shutdown()
  }

  /** immutable snapshot of client usage statistics */
  def stats() = underlying.stats()

  /** underlying [[org.jboss.netty.util.Timer]] used by the async client */
  def timer: Timer = underlying.timer

  /** @see com.datasift.dropwizard.hbase.HBase.unlockRow() */
  def unlockRow(lock: RowLock): Deferred[AnyRef] = withPermit {
    underlying.unlockRow(lock)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.newScanner() */
  def newScanner(table: Array[Byte]): RowScanner = {
    new BoundedRowScanner(underlying.newScanner(table), semaphore)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.newScanner() */
  def newScanner(table: String): RowScanner = {
    new BoundedRowScanner(underlying.newScanner(table), semaphore)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.newScanner() */
  def newScanner(table: Array[Byte], config: ScannerConfiguration): RowScanner = {
    new BoundedRowScanner(underlying.newScanner(table, config), semaphore)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.newScanner() */
  def newScanner(table: String, config: ScannerConfiguration): RowScanner = {
    new BoundedRowScanner(underlying.newScanner(table, config), semaphore)
  }

  /** utility for executing an asynchronous request that requires and holds a permit */
  private def withPermit[A](f: => Deferred[A]): Deferred[A] = {
    semaphore.acquire()
    f.addBoth(new PermitReleasingCallback[A](semaphore))
  }
}
