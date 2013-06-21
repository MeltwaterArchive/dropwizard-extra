package com.datasift.dropwizard.hbase

import com.codahale.dropwizard.setup.Environment

/** Factory object for [[com.datasift.dropwizard.hbase.HBaseClient]] instances */
object HBase {

  /** Creates an [[com.datasift.dropwizard.hbase.HBaseClient]] from the given configuration.
    *
    * @param env the environment to manage the HBase client lifecycle.
    * @return a configured and managed [[com.datasift.dropwizard.hbase.HBaseClient]].
    */
  def apply(env: Environment): HBaseClient = {
    new HBaseClientFactory().build(env)
  }

  /** Creates an [[com.datasift.dropwizard.hbase.HBaseClient]] from the given configuration.
    *
    * @param env the environment to manage the HBase client lifecycle.
    * @param name a name for the HBase client instance.
    * @return a configured and managed [[com.datasift.dropwizard.hbase.HBaseClient]].
    */
  def apply(env: Environment, name: String): HBaseClient = {
    new HBaseClientFactory().build(env, name)
  }
}
