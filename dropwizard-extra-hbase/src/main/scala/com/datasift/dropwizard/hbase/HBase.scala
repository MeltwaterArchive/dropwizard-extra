package com.datasift.dropwizard.hbase

import com.yammer.dropwizard.lifecycle.Managed
import com.yammer.dropwizard.config.Environment
import config.{ScannerConfiguration, HBaseConfiguration, HBaseClientConfiguration}
import org.jboss.netty.util.Timer
import java.util.ArrayList
import org.hbase.async._
import com.stumbleupon.async.{TimeoutException, Deferred}
import scanner.RowScanner
import com.datasift.dropwizard.health.HBaseHealthCheck

/** Factory companion object for [[com.datasift.dropwizard.hbase.HBase]] instances */
object HBase {

  /** creates, instruments and manages an [[com.datasift.dropwizard.hbase.HBase]]
   *
   * @param conf configuration for the HBase instance
   * @param env environment for the [[com.yammer.dropwizard.Service]] using HBase
   */
  def apply(conf: HBaseClientConfiguration, env: Environment): HBase = {
    val client = new HBaseClient(conf.zookeeper.quorumSpec)
    client.setFlushInterval(conf.flushInterval.toMilliseconds.toShort)
    client.setIncrementBufferSize(conf.incrementBufferSize.toBytes.toInt)

    // determine the appropriate HBase type to use
    val hbase = if (conf.maxConcurrentRequests == 0) {
      new InstrumentedHBase(new HBaseProxy(client))
    } else {
      new BoundedHBase(
        new InstrumentedHBase(new HBaseProxy(client)),
        conf.maxConcurrentRequests
      )
    }

    // add healthchecks for META and ROOT tables
    env.addHealthCheck(new HBaseHealthCheck(hbase, ".META."))
    env.addHealthCheck(new HBaseHealthCheck(hbase, "-ROOT-"))

    env.manage(new ManagedHBase(hbase, conf))
    hbase
  }

  /** creates, instruments and manages an [[com.datasift.dropwizard.hbase.HBase]]
   *
   * @param conf configuration of the [[com.yammer.dropwizard.Service]] using HBase
   * @param env environment for the [[com.yammer.dropwizard.Service]] using HBase
   */
  def apply[A <: HBaseConfiguration](conf: A, env: Environment): HBase = {
    apply(conf.hbase, env)
  }

  /** manages an [[com.datasift.dropwizard.hbase.HBase]] instance */
  private class ManagedHBase(hbase: HBase, conf: HBaseClientConfiguration)
    extends Managed {

    /** initializes and starts the [[com.datasift.dropwizard.hbase.HBase]] instance
     *
     * Note: since the asynchronous client lazily connects, a connection is
     * forced by checking for the existence of a dummy table.
     */
    def start() {
      // dummy request to force the client to connect to the cluster synchronously
      try {
        hbase.ensureTableExists(".META.").join(conf.connectionTimeout.toMilliseconds)
      } catch {
        case _: TableNotFoundException => // we're expecting this
        case e: TimeoutException =>
          throw new RuntimeException("Failed to connect to connect to HBase cluster after %s".format(conf.connectionTimeout), e)
      }
    }

    /** shutsdown the [[org.hbase.async.HBaseClient]] */
    def stop() {
      hbase.shutdown().join()
    }
  }
}

/** Client for interacting with an HBase cluster
 *
 * All implementations are wrapper proxies around [[org.hbase.async.HBaseClient]]
 * providing a slightly more idiomatic API and other useful functionality.
 *
 * @see org.hbase.async.HBaseClient
 */
trait HBase {

  /** maximum time in milliseconds for which edits can be buffered */
  def flushInterval: Short

  /** capacity of the increment buffer */
  def incrementBufferSize: Int

  /** atomically creates a cell if, and only if, it doesn't already exist */
  def create(edit: PutRequest): Deferred[Boolean]

  /** buffer a durable increment for coalescing */
  def bufferedIncrement(request: AtomicIncrementRequest): Deferred[Long]

  /** atomically and durably increments a value */
  def increment(request: AtomicIncrementRequest): Deferred[Long]

  /** atomically increments a value, with optional durability */
  def increment(request: AtomicIncrementRequest, durable: Boolean): Deferred[Long]

  /** atomically compares and sets (CAS) a single cell */
  def compareAndSet(edit: PutRequest, expected: Array[Byte]): Deferred[Boolean]

  /** atomically compares and sets (CAS) a single cell */
  def compareAndSet(edit: PutRequest, expected: String): Deferred[Boolean]

  /** deletes the specified cells */
  def delete(request: DeleteRequest): Deferred[AnyRef]

  /** ensures that a specific table exists */
  def ensureTableExists(table: Array[Byte]): Deferred[AnyRef]

  /** ensures that a specific table exists */
  def ensureTableExists(table: String): Deferred[AnyRef]

  /** ensures that a specific family within a table exists */
  def ensureTableFamilyExists(table: Array[Byte], family: Array[Byte]): Deferred[AnyRef]

  /** ensures that a specific family within a table exists */
  def ensureTableFamilyExists(table: String, family: String): Deferred[AnyRef]

  /** flushes all requests buffered on the client-side */
  def flush(): Deferred[AnyRef]

  /** retrieves the specified cells */
  def get(request: GetRequest): Deferred[ArrayList[KeyValue]]

  /** acquires an explicit row lock */
  def lockRow(request: RowLockRequest): Deferred[RowLock]

  /** creates a new [[com.datasift.dropwizard.hbase.scanner.RowScanner]] for a table with defaults */
  def newScanner(table: Array[Byte]): RowScanner

  /** creates a new [[com.datasift.dropwizard.hbase.scanner.RowScanner]] for a table with defaults  */
  def newScanner(table: String): RowScanner

  /** creates a new configured [[com.datasift.dropwizard.hbase.scanner.RowScanner]] for a table */
  def newScanner(table: Array[Byte], config: ScannerConfiguration): RowScanner

  /** creates a new configured [[com.datasift.dropwizard.hbase.scanner.RowScanner]] for a table */
  def newScanner(table: String, config: ScannerConfiguration): RowScanner

  /** stores the specified cells */
  def put(request: PutRequest): Deferred[AnyRef]

  /** performs a graceful shutdown of this client, flushing any pending requests */
  def shutdown(): Deferred[AnyRef]

  /** immutable snapshot of client usage statistics */
  def stats(): ClientStats

  /** underlying [[org.jboss.netty.util.Timer]] used by the async client */
  def timer: Timer

  /** releases an explicit row lock */
  def unlockRow(lock: RowLock): Deferred[AnyRef]
}
