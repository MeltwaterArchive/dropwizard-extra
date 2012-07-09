package com.datasift.dropwizard.db

import com.yammer.dropwizard.db.{DatabaseFactory, DatabaseConfiguration}
import com.yammer.dropwizard.config.Environment

/**
 * Factory for constructing a [[com.yammer.dropwizard.db.Database]].
 */
object Database {

  /**
   * Creates a [[com.yammer.dropwizard.db.Database]] for a given [[com.yammer.dropwizard.config.Configuration]].
   *
   * @param conf configuration to configure the [[com.yammer.dropwizard.db.Database]] with
   * @param env [[com.yammer.dropwizard.config.Environment]] to manage the [[com.yammer.dropwizard.db.Database]] lifecycle.
   * @return a configured and managed [[com.yammer.dropwizard.db.Database]] instance.
   */
  def apply(conf: DatabaseConfiguration, env: Environment): com.yammer.dropwizard.db.Database = {
    new DatabaseFactory(env).build(conf, conf.getUrl)
  }
}
