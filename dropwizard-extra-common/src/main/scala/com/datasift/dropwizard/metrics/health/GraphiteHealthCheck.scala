package com.datasift.dropwizard.metrics.health

import com.datasift.dropwizard.health.SocketHealthCheck

/**[[com.yammer.metrics.core.HealthCheck]] for a Graphite connection */
class GraphiteHealthCheck(host: String, port: Int)
  extends SocketHealthCheck(host, port, "Graphite")
