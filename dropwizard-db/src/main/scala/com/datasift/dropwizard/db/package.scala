package com.datasift.dropwizard

import com.yammer.dropwizard.config.Environment
import com.yammer.dropwizard.db.{DatabaseConfiguration, DatabaseFactory, Database}
import db.config.SimpleDatabaseConfiguration

package object db {

  /** implicit wrapper for a [[com.yammer.dropwizard.db.Database]] */
  implicit def wrapDatabase(db: Database): DatabaseWrapper = {
    new DatabaseWrapper(db)
  }

  class DatabaseWrapper(db: Database) {

    /** creates a DAO instance
     *
     * @tparam T type of the DAO to create
     */
    def daoFor[T : Manifest]: T = {
      db.onDemand[T](implicitly[Manifest[T]].erasure.asInstanceOf[Class[T]])
    }
  }

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
}
