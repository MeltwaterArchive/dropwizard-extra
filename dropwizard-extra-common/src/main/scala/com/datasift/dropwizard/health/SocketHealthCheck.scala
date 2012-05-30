package com.datasift.dropwizard.health

import com.yammer.metrics.core.HealthCheck
import java.net.Socket
import com.yammer.metrics.core.HealthCheck.Result

/** [[com.yammer.metrics.core.HealthCheck]] for a socket connection */
abstract class SocketHealthCheck(host: String, port: Int, name: String)
  extends HealthCheck(name + ": " + host + ":" + port) {

  override protected def check = {
    try {
      if (new Socket(host, port).isConnected) {
        Result.healthy
      } else {
        Result.unhealthy("Not connected: unknown problem")
      }
    } catch {
      case t: Throwable => Result.unhealthy(t)
    }
  }
}
