package com.datasift.dropwizard.jdbi

import com.codahale.dropwizard.db.DatabaseConfiguration
import com.codahale.dropwizard.setup.Environment
import com.codahale.dropwizard.jdbi.DBIFactory

/**
 * Factory for constructing a [[com.codahale.dropwizard.jdbi]].
 */
object DBI {

  /**
   * Creates a [[org.skife.jdbi.v2.DBI]] for a given [[com.codahale.dropwizard.db.DatabaseConfiguration]].
   *
   * @param conf configuration to configure the [[org.skife.jdbi.v2.DBI]] with
   * @param env [[com.codahale.dropwizard.setup.Environment]] to manage the [[org.skife.jdbi.v2.DBI]] lifecycle.
   * @return a configured and managed [[org.skife.jdbi.v2.DBI]] instance.
   */
  def apply(conf: DatabaseConfiguration, env: Environment): org.skife.jdbi.v2.DBI = {
    new DBIFactory().build(env, conf, conf.getUrl)
  }
}
