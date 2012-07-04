package com.datasift.dropwizard.db

import com.yammer.dropwizard.db.Database

class DatabaseWrapper(db: Database) {

  /** creates a DAO instance
   *
   * @tparam T type of the DAO to create
   */
  def daoFor[T : Manifest]: T = {
    db.onDemand[T](implicitly[Manifest[T]].erasure.asInstanceOf[Class[T]])
  }
}
