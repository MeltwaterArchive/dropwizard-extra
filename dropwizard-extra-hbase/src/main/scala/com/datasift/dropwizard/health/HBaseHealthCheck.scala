package com.datasift.dropwizard.health

import com.yammer.metrics.core.HealthCheck
import com.yammer.metrics.core.HealthCheck.Result
import com.datasift.dropwizard.hbase.HBase

/** [[com.yammer.metrics.core.HealthCheck]] for an HBase connection to a table */
class HBaseHealthCheck(hbase: HBase, table: String) extends HealthCheck("HBase: " + table) {

  private val tableBytes = table.getBytes

  override def check: Result = {
    hbase.ensureTableExists(tableBytes).joinUninterruptibly()
    Result.healthy
  }
}
