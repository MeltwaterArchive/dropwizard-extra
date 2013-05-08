package com.datasift.dropwizard.jdbi

import com.codahale.dropwizard.db.DatabaseConfiguration
import com.codahale.dropwizard.setup.Environment
import com.codahale.dropwizard.jdbi.DBIFactory

/** Factory object for [[org.skife.jdbi.v2.DBI]] instances. */
object DBI {

  /** Creates a [[org.skife.jdbi.v2.DBI]] from the given configuration.
    *
    * @param conf configuration for the database connection.
    * @param env environment to manage the database connection lifecycle.
    * @return a configured and managed [[org.skife.jdbi.v2.DBI]] instance.
    */
  def apply(conf: DatabaseConfiguration, env: Environment): org.skife.jdbi.v2.DBI = {
    new DBIFactory().build(env, conf, conf.getUrl)
  }
}

/** Provides idiomatic Scala enhancements to the JDBI API.
  *
  * @param db the [[org.skife.jdbi.v2.DBI]] instance to wrap.
  */
class DBI(db: org.skife.jdbi.v2.DBI) {

  /** Creates a typed DAO instance.
    *
    * @tparam T type of the DAO to create.
    * @return a DAO instance for the specified type.
    */
  def daoFor[T : Manifest]: T = {
    db.onDemand[T](manifest[T].erasure.asInstanceOf[Class[T]])
  }
}
