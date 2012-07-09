package com.datasift.dropwizard.db

/**
 * Provides enhancements to the Dropwizard DB API.
 *
 * @param db [[com.yammer.dropwizard.db.Database]] to wrap
 */
class DatabaseWrapper(db: com.yammer.dropwizard.db.Database) {

  /**
   * Creates a DAO instance.
   *
   * @tparam T type of the DAO to create
   * @return a DAO instance for the specified type
   */
  def daoFor[T : Manifest]: T = {
    db.onDemand[T](implicitly[Manifest[T]].erasure.asInstanceOf[Class[T]])
  }
}
