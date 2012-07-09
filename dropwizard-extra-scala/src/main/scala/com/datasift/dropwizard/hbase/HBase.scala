package com.datasift.dropwizard.hbase

import config.HBaseClientConfiguration
import com.yammer.dropwizard.config.Environment

/**
 * Factory object for [[com.datasift.dropwizard.hbase.HBaseClient]] instances
 * */
object HBase {

  /**
   * Create an [[com.datasift.dropwizard.hbase.HBaseClient]] from the given [[com.datasift.dropwizard.hbase.config.HBaseClientConfiguration]].
   *
   * @param conf configuration for the [[com.datasift.dropwizard.hbase.HBaseClient]]
   * @param env [[com.yammer.dropwizard.config.Environment]] to manage the [[com.datasift.dropwizard.hbase.HBaseClient]]
   * @return a configured and managed [[com.datasift.dropwizard.hbase.HBaseClient]]
   */
  def apply(conf: HBaseClientConfiguration, env: Environment): HBaseClient = {
    new HBaseClientFactory(env).build(conf)
  }
}
