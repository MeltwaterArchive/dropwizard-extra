package com.datasift.dropwizard.jdbi

/**
 * Provides enhancements to the Dropwizard DB API.
 *
 * @param db [[org.skife.jdbi.v2.DBI]] to wrap
 */
class DBIWrapper(db: org.skife.jdbi.v2.DBI) {

  /**
   * Creates a DAO instance.
   *
   * @tparam T type of the DAO to create
   * @return a DAO instance for the specified type
   */
  def daoFor[T : Manifest]: T = {
    db.onDemand[T](manifest[T].erasure.asInstanceOf[Class[T]])
  }
}
