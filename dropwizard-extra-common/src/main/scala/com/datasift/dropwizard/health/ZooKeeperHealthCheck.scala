package com.datasift.dropwizard.health

import com.yammer.metrics.core.HealthCheck
import com.yammer.metrics.core.HealthCheck.Result
import java.net.Socket

/** [[com.yammer.metrics.core.HealthCheck]] for a ZooKeeper Quorum */
class ZooKeeperHealthCheck(hosts: Set[String], port: Int)
  extends HealthCheck("ZooKeeper: " + hosts.map(_ + ":" + port).mkString(", ")) {

  override protected def check = {
    val connected = hosts filter {
      new Socket(_, port).isConnected
    }

    if (connected.isEmpty) {
      // total failure
      Result.unhealthy("Unable to connect to any nodes in quorum: " +
        hosts.mkString(", "))
    } else if (connected.size < hosts.size) {
      // partial failure
      Result.healthy("Unable to connect to some nodes in quorum: " +
        hosts.diff(connected).mkString(", "))
    } else {
      // everything's fine
      Result.healthy()
    }
  }
}
