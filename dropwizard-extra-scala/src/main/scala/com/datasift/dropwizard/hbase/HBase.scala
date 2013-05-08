package com.datasift.dropwizard.hbase

import config.HBaseClientConfiguration
import com.codahale.dropwizard.setup.Environment

/** Factory object for [[com.datasift.dropwizard.hbase.HBaseClient]] instances */
object HBase {

  /** Creates an [[com.datasift.dropwizard.hbase.HBaseClient]] from the given configuration.
    *
    * @param conf configuration for the client.
    * @param env the environment to manage the HBase client lifecycle.
    * @return a configured and managed [[com.datasift.dropwizard.hbase.HBaseClient]].
    */
  def apply(conf: HBaseClientConfiguration, env: Environment): HBaseClient = {
    new HBaseClientFactory(env).build(conf)
  }
}
