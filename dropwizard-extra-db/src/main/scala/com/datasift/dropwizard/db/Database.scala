package com.datasift.dropwizard.db

import com.yammer.dropwizard.db.{DatabaseFactory, Database, DatabaseConfiguration}
import com.yammer.dropwizard.config.Environment
import config.SimpleDatabaseConfiguration

/** factory methods for constructing a [[com.yammer.dropwizard.db.Database]] */
object Database {

  /** creates a [[com.yammer.dropwizard.db.Database]] for a [[com.yammer.dropwizard.config.Configuration]] */
  def apply(conf: DatabaseConfiguration, env: Environment): Database = {
    new DatabaseFactory(env).build(conf, conf.getUrl)
  }

  /** creates a [[com.yammer.dropwizard.db.Database]] for a [[com.yammer.dropwizard.Service]] [[com.yammer.dropwizard.config.Configuration]] */
  def apply[A <: SimpleDatabaseConfiguration](conf: A, env: Environment): Database = {
    apply(conf.database, env)
  }
}
