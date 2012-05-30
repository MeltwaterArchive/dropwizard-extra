package com.datasift.dropwizard.health

/** [[com.yammer.metrics.core.HealthCheck]] for a Graphite connection */
class GraphiteHealthCheck(host: String, port: Int)
  extends SocketHealthCheck(host, port, "Graphite")
