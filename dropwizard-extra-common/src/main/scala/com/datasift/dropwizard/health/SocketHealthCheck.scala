package com.datasift.dropwizard.health

import com.yammer.metrics.core.HealthCheck
import com.yammer.metrics.core.HealthCheck.Result
import java.net.Socket

/** [[com.yammer.metrics.core.HealthCheck]] for a socket connection */
abstract class SocketHealthCheck(host: String, port: Int, name: String)
  extends HealthCheck(name + ": " + host + ":" + port) {

  override protected def check = {
    if (new Socket(host, port).isConnected) {
      Result.healthy
    } else {
      Result.unhealthy("Not connected: unknown problem")
    }
  }
}
