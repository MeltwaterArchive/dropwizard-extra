package com.datasift.dropwizard.health

import com.yammer.metrics.core.HealthCheck
import com.yammer.metrics.core.HealthCheck.Result
import org.hbase.async.{NoSuchColumnFamilyException, TableNotFoundException, HBaseClient}

/** [[com.yammer.metrics.core.HealthCheck]] for an HBase connection to a table */
class HBaseHealthCheck(hbase: HBaseClient) extends HealthCheck("HBase") {

  override def check: Result = {
    hbase.ensureTableExists(HBaseClient.EMPTY_ARRAY).join()
    Result.healthy
  }
}
