package com.datasift.dropwizard.hbase

import com.stumbleupon.async.Deferred
import config.ScannerConfiguration
import java.util.ArrayList
import org.hbase.async._
import org.jboss.netty.util.Timer
import scanner.{RowScanner, RowScannerProxy}

/** Client for interacting with an HBase cluster
 *
 * You may not create an instance of this class directly. Instead, use the factory
 * provided by [[com.datasift.dropwizard.hbase.HBase.apply()]]
 *
 * This client is a simple proxy that provides a more idiomatic API to
 * [[org.hbase.async.HBaseClient]].
 *
 * @param client underlying client for interacting with the HBase cluster
 */
class HBaseProxy private[hbase] (client: HBaseClient) extends HBase {

  /** maximum time in milliseconds for which edits can be buffered */
  val flushInterval: Short = client.getFlushInterval

  /** capacity of the increment buffer */
  val incrementBufferSize: Int = client.getIncrementBufferSize

  /** underlying [[org.jboss.netty.util.Timer]] used by the async client */
  def timer: Timer = client.getTimer

  /** immutable snapshot of client usage statistics */
  def stats(): ClientStats = {
    client.stats()
  }

  /** @see com.datasift.dropwizard.hbase.HBase.create() */
  def create(edit: PutRequest): Deferred[Boolean] = {
    client.atomicCreate(edit).asInstanceOf[Deferred[Boolean]]
  }

  /** @see com.datasift.dropwizard.hbase.HBase.bufferedIncrement() */
  def bufferedIncrement(request: AtomicIncrementRequest): Deferred[Long] = {
    client.bufferAtomicIncrement(request).asInstanceOf[Deferred[Long]]
  }

  /** @see com.datasift.dropwizard.hbase.HBase.increment() */
  def increment(request: AtomicIncrementRequest): Deferred[Long] = {
    client.atomicIncrement(request).asInstanceOf[Deferred[Long]]
  }

  /** @see com.datasift.dropwizard.hbase.HBase.increment() */
  def increment(request: AtomicIncrementRequest, durable: Boolean): Deferred[Long] = {
    client.atomicIncrement(request, durable).asInstanceOf[Deferred[Long]]
  }

  /** @see com.datasift.dropwizard.hbase.HBase.compareAndSet() */
  def compareAndSet(edit: PutRequest, expected: Array[Byte]): Deferred[Boolean] = {
    client.compareAndSet(edit, expected).asInstanceOf[Deferred[Boolean]]
  }

  /** @see com.datasift.dropwizard.hbase.HBase.compareAndSet() */
  def compareAndSet(edit: PutRequest, expected: String): Deferred[Boolean] = {
    client.compareAndSet(edit, expected).asInstanceOf[Deferred[Boolean]]
  }

  /** @see com.datasift.dropwizard.hbase.HBase.delete() */
  def delete(request: DeleteRequest): Deferred[AnyRef] = {
    client.delete(request)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.ensureTableExists() */
  def ensureTableExists(table: Array[Byte]): Deferred[AnyRef] = {
    client.ensureTableExists(table)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.ensureTableExists() */
  def ensureTableExists(table: String): Deferred[AnyRef] = {
    client.ensureTableExists(table)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.ensureTableFamilyExists() */
  def ensureTableFamilyExists(table: Array[Byte], family: Array[Byte]): Deferred[AnyRef] = {
    client.ensureTableFamilyExists(table, family)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.ensureTableFamilyExists() */
  def ensureTableFamilyExists(table: String, family: String): Deferred[AnyRef] = {
    client.ensureTableFamilyExists(table, family)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.flush() */
  def flush(): Deferred[AnyRef] = {
    client.flush()
  }

  /** @see com.datasift.dropwizard.hbase.HBase.get() */
  def get(request: GetRequest): Deferred[ArrayList[KeyValue]] = {
    client.get(request)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.lockRow() */
  def lockRow(request: RowLockRequest): Deferred[RowLock] = {
    client.lockRow(request)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.put() */
  def put(request: PutRequest): Deferred[AnyRef] = {
    client.put(request)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.shutdown() */
  def shutdown(): Deferred[AnyRef] = {
    client.shutdown()
  }

  /** @see com.datasift.dropwizard.hbase.HBase.unlockRow() */
  def unlockRow(lock: RowLock): Deferred[AnyRef] = {
    client.unlockRow(lock)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.newScanner() */
  def newScanner(table: Array[Byte]): RowScanner = {
    new RowScannerProxy(client.newScanner(table), new ScannerConfiguration)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.newScanner() */
  def newScanner(table: String): RowScanner = {
    new RowScannerProxy(client.newScanner(table), new ScannerConfiguration)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.newScanner() */
  def newScanner(table: Array[Byte], config: ScannerConfiguration): RowScanner = {
    new RowScannerProxy(client.newScanner(table), config)
  }

  /** @see com.datasift.dropwizard.hbase.HBase.newScanner() */
  def newScanner(table: String, config: ScannerConfiguration): RowScanner = {
    new RowScannerProxy(client.newScanner(table), config)
  }
}
