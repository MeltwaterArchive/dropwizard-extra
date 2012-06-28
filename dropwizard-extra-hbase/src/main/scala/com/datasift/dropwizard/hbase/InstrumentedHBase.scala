package com.datasift.dropwizard.hbase

import config.ScannerConfiguration
import org.hbase.async._
import scanner.InstrumentedRowScanner
import com.yammer.metrics.scala.{Timer, Instrumented}
import com.stumbleupon.async.Deferred

/** Instruments an [[com.datasift.dropwizard.hbase.HBase]] with [[com.yammer.metrics]] */
class InstrumentedHBase(underlying: HBase) extends HBase with Instrumented {

  // create timers for each request type
  private val creates = metrics.timer("create", "requests")
  private val increments = metrics.timer("increment", "requests")
  private val compareAndSets = metrics.timer("compareAndSet", "requests")
  private val deletes = metrics.timer("delete", "requests")
  private val assertions = metrics.timer("assertion", "requests")
  private val flushes = metrics.timer("flush", "requests")
  private val gets = metrics.timer("get", "requests")
  private val locks = metrics.timer("lock", "requests")
  private val puts = metrics.timer("put", "requests")
  private val unlocks = metrics.timer("unlock", "requests")

  // general stats
  metrics.gauge("atomicIncrements")       { stats().atomicIncrements }
  metrics.gauge("connectionsCreated")     { stats().connectionsCreated }
  metrics.gauge("contendedMetaLookups")   { stats().contendedMetaLookups }
  metrics.gauge("deletes")                { stats().deletes }
  metrics.gauge("flushes")                { stats().flushes }
  metrics.gauge("gets")                   { stats().gets }
  metrics.gauge("noSuchRegionExceptions") { stats().noSuchRegionExceptions}
  metrics.gauge("numBatchedRpcSent")      { stats().numBatchedRpcSent }
  metrics.gauge("numRpcDelayedDueToNSRE") { stats().numRpcDelayedDueToNSRE }
  metrics.gauge("puts")                   { stats().puts }
  metrics.gauge("rootLookups")            { stats().rootLookups }
  metrics.gauge("rowLocks")               { stats().rowLocks }
  metrics.gauge("scannersOpened")         { stats().scannersOpened }
  metrics.gauge("scans")                  { stats().scans }
  metrics.gauge("uncontendedMetaLookups") { stats().uncontendedMetaLookups }

  // increment buffer stats
  metrics.gauge("averageLoadPenalty", "incrementBuffer") {
    stats().incrementBufferStats().averageLoadPenalty }
  metrics.gauge("evictionCount", "incrementBuffer") {
    stats().incrementBufferStats().evictionCount }
  metrics.gauge("hitCount", "incrementBuffer") {
    stats().incrementBufferStats().hitCount }
  metrics.gauge("hitRate", "incrementBuffer") {
    stats().incrementBufferStats().hitRate }
  metrics.gauge("loadCount", "incrementBuffer") {
    stats().incrementBufferStats().loadCount }
  metrics.gauge("loadExceptionCount", "incrementBuffer") {
    stats().incrementBufferStats().loadExceptionCount }
  metrics.gauge("loadExceptionRate", "incrementBuffer") {
    stats().incrementBufferStats().loadExceptionRate }
  metrics.gauge("loadSuccessCount", "incrementBuffer") {
    stats().incrementBufferStats().loadSuccessCount }
  metrics.gauge("missCount", "incrementBuffer") {
    stats().incrementBufferStats().missCount }
  metrics.gauge("missRate", "incrementBuffer") {
    stats().incrementBufferStats().missRate }
  metrics.gauge("requestCount", "incrementBuffer") {
    stats().incrementBufferStats().requestCount }
  metrics.gauge("totalLoadTime", "incrementBuffer") {
    stats().incrementBufferStats().totalLoadTime }

  /** maximum time in milliseconds for which edits can be buffered */
  val flushInterval: Short = underlying.flushInterval

  /** capacity of the increment buffer */
  val incrementBufferSize: Int = underlying.incrementBufferSize

  /** atomically creates a cell if, and only if, it doesn't already exist */
  def create(edit: PutRequest) = withTimer(creates) {
    underlying.create(edit)
  }

  /** buffer a durable increment for coalescing */
  def bufferedIncrement(request: AtomicIncrementRequest) = withTimer(increments) {
    underlying.bufferedIncrement(request)
  }

  /** atomically and durably increments a value */
  def increment(request: AtomicIncrementRequest) = withTimer(increments) {
    underlying.increment(request)
  }

  /** atomically increments a value, with optional durability */
  def increment(request: AtomicIncrementRequest, durable: Boolean) = withTimer(increments) {
    underlying.increment(request, durable)
  }

  /** atomically compares and sets (CAS) a single cell */
  def compareAndSet(edit: PutRequest, expected: Array[Byte]) = withTimer(compareAndSets) {
    underlying.compareAndSet(edit, expected)
  }

  /** atomically compares and sets (CAS) a single cell */
  def compareAndSet(edit: PutRequest, expected: String) = withTimer(compareAndSets) {
    underlying.compareAndSet(edit, expected)
  }

  /** deletes the specified cells */
  def delete(request: DeleteRequest) = withTimer(deletes) {
    underlying.delete(request)
  }

  /** ensures that a specific table exists */
  def ensureTableExists(table: Array[Byte]) = withTimer(assertions) {
    underlying.ensureTableExists(table)
  }

  /** ensures that a specific table exists */
  def ensureTableExists(table: String) = withTimer(assertions) {
    underlying.ensureTableExists(table)
  }

  /** ensures that a specific family within a table exists */
  def ensureTableFamilyExists(table: Array[Byte], family: Array[Byte]) = withTimer(assertions) {
    underlying.ensureTableFamilyExists(table, family)
  }

  /** ensures that a specific family within a table exists */
  def ensureTableFamilyExists(table: String, family: String) = withTimer(assertions) {
    underlying.ensureTableFamilyExists(table, family)
  }

  /** flushes all requests buffered on the client-side */
  def flush() = withTimer(flushes) {
    underlying.flush()
  }

  /** retrieves the specified cells */
  def get(request: GetRequest) = withTimer(gets) {
    underlying.get(request)
  }

  /** acquires an explicit row lock */
  def lockRow(request: RowLockRequest) = withTimer(locks) {
    underlying.lockRow(request)
  }

  /** creates a new [[com.datasift.dropwizard.hbase.scanner.RowScanner]] for a table*/
  def newScanner(table: Array[Byte]) = {
    new InstrumentedRowScanner(underlying.newScanner(table))
  }

  /** creates a new [[com.datasift.dropwizard.hbase.scanner.RowScanner]] for a table*/
  def newScanner(table: String) = {
    new InstrumentedRowScanner(underlying.newScanner(table))
  }

  /** creates a new [[com.datasift.dropwizard.hbase.scanner.RowScanner]] for a table*/
  def newScanner(table: Array[Byte], config: ScannerConfiguration) = {
    new InstrumentedRowScanner(underlying.newScanner(table, config))
  }

  /** creates a new [[com.datasift.dropwizard.hbase.scanner.RowScanner]] for a table*/
  def newScanner(table: String, config: ScannerConfiguration) = {
    new InstrumentedRowScanner(underlying.newScanner(table, config))
  }

  /** stores the specified cells */
  def put(request: PutRequest) = withTimer(puts) {
    underlying.put(request)
  }

  /** performs a graceful shutdown of this client, flushing any pending requests */
  def shutdown() = underlying.shutdown()

  /** immutable snapshot of client usage statistics */
  def stats() = underlying.stats()

  /** underlying [[org.jboss.netty.util.Timer]] used by the async client */
  def timer: org.jboss.netty.util.Timer = underlying.timer

  /** releases an explicit row lock */
  def unlockRow(lock: RowLock) = withTimer(unlocks) {
    underlying.unlockRow(lock)
  }

  /** time the execution of an asynchrnous function
   *
   * The timer will stop when the callback is called for the function
   *
   * @param timer timer to time the function
   * @param f function to time
   * @tparam A return type of function's Deferred
   * @return the result of the function
   */
  private def withTimer[A](timer: => Timer)(f: Deferred[A]): Deferred[A] = {
    val ctx = timer.timerContext()
    f.addBoth(new TimerStoppingCallback[A](ctx))
  }
}
