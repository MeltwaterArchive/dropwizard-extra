package com.datasift.dropwizard.jdbi

import com.yammer.dropwizard.db.DatabaseConfiguration
import com.yammer.dropwizard.config.Environment
import com.yammer.dropwizard.jdbi.DBIFactory

/**
 * Factory for constructing a [[com.yammer.dropwizard.jdbi]].
 */
object DBI {

  /**
   * Creates a [[org.skife.jdbi.v2.DBI]] for a given [[com.yammer.dropwizard.db.DatabaseConfiguration]].
   *
   * @param conf configuration to configure the [[org.skife.jdbi.v2.DBI]] with
   * @param env [[com.yammer.dropwizard.config.Environment]] to manage the [[org.skife.jdbi.v2.DBI]] lifecycle.
   * @return a configured and managed [[org.skife.jdbi.v2.DBI]] instance.
   */
  def apply(conf: DatabaseConfiguration, env: Environment): org.skife.jdbi.v2.DBI = {
    new DBIFactory().build(env, conf, conf.getUrl)
  }
}
