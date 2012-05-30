package com.datasift.dropwizard.hbase

import com.datasift.dropwizard.conf.ConfigurableHBaseClient
import com.datasift.dropwizard.ComposableService
import com.yammer.dropwizard.lifecycle.Managed
import org.hbase.async.HBaseClient
import com.yammer.dropwizard.config.{Configuration, Environment}
import com.yammer.dropwizard.Logging

/** Manage an HBaseClient for a [[com.datasift.dropwizard.ComposableService]] */
trait ManagedHBaseClient[T <: Configuration with ConfigurableHBaseClient] {
  self: ComposableService[T] with Logging =>

  var hbase: HBaseClient = null

  self beforeInit { (conf: T, env: Environment) =>
    hbase = new HBaseClient(conf.hbase.zookeeper.quorumSpec)

    env.manage(new Managed {
      def stop() {
        log.info("Shutting down HBase Client")
        hbase.shutdown().join()
        log.info("Shutdown HBase Client")
      }

      def start() {
        log.info("Using ZooKeeper quorum at {} on port {} for HBase",
          List(conf.hbase.zookeeper.hosts.mkString(", "),
            conf.hbase.zookeeper.port.toString): _*)
      }
    })
  }

  // todo: add healthchecks
}
