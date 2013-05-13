package com.datasift.dropwizard.jdbi

import com.codahale.dropwizard.db.DatabaseConfiguration
import com.codahale.dropwizard.setup.Environment
import com.codahale.dropwizard.jdbi.DBIFactory
import org.skife.jdbi.v2.{TransactionIsolationLevel, TransactionCallback, TransactionStatus, Handle}

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

  /** Executes the given function within a transaction.
    *
    * @tparam A the return type of the function to execute.
    * @param f the function to execute within the transaction.
    * @return the result of the function.
    * @throws Exception if an Exception is thrown by the function, the transaction will be
    *                   rolled-back.
    */
  def inTransaction[A](f: (Handle, TransactionStatus) => A): A = {
    db.inTransaction(new TransactionCallback[A] {
      def inTransaction(handle: Handle, status: TransactionStatus): A = f(handle, status)
    })
  }

  /** Executes the given function within a transaction.
    *
    * @tparam A the return type of the function to execute.
    * @param f the function to execute within the transaction.
    * @return the result of the function.
    * @throws Exception if an Exception is thrown by the function, the transaction will be
    *                   rolled-back.
    */
  def inTransaction[A](f: Handle => A): A = {
    db.inTransaction(new TransactionCallback[A] {
      def inTransaction(handle: Handle, status: TransactionStatus): A = f(handle)
    })
  }

  /** Executes the given function within a transaction.
    *
    * @tparam A the return type of the function to execute.
    * @param f the function to execute within the transaction.
    * @return the result of the function.
    * @throws Exception if an Exception is thrown by the function, the transaction will be
    *                   rolled-back.
    */
  def inTransaction[A](f: TransactionStatus => A): A = {
    db.inTransaction(new TransactionCallback[A] {
      def inTransaction(handle: Handle, status: TransactionStatus): A = f(status)
    })
  }

  /** Executes the given function within a transaction of the given isolation level.
    *
    * @tparam A the return type of the function to execute.
    * @param isolation the isolation level for the transaction.
    * @param f the function to execute within the transaction.
    * @return the result of the function.
    * @throws Exception if an Exception is thrown by the function, the transaction will be
    *                   rolled-back.
    */
  def inTransaction[A](isolation: TransactionIsolationLevel)
                      (f: (Handle, TransactionStatus) => A): A = {
    db.inTransaction(isolation, new TransactionCallback[A] {
      def inTransaction(handle: Handle, status: TransactionStatus): A = f(handle, status)
    })
  }

  /** Executes the given function within a transaction of the given isolation level.
    *
    * @tparam A the return type of the function to execute.
    * @param isolation the isolation level for the transaction.
    * @param f the function to execute within the transaction.
    * @return the result of the function.
    * @throws Exception if an Exception is thrown by the function, the transaction will be
    *                   rolled-back.
    */
  def inTransaction[A](isolation: TransactionIsolationLevel)
                      (f: Handle => A): A = {
    db.inTransaction(isolation, new TransactionCallback[A] {
      def inTransaction(handle: Handle, status: TransactionStatus): A = f(handle)
    })
  }

  /** Executes the given function within a transaction of the given isolation level.
    *
    * @tparam A the return type of the function to execute.
    * @param isolation the isolation level for the transaction.
    * @param f the function to execute within the transaction.
    * @return the result of the function.
    * @throws Exception if an Exception is thrown by the function, the transaction will be
    *                   rolled-back.
    */
  def inTransaction[A](isolation: TransactionIsolationLevel)
                      (f: TransactionStatus => A): A = {
    db.inTransaction(isolation, new TransactionCallback[A] {
      def inTransaction(handle: Handle, status: TransactionStatus): A = f(status)
    })
  }
}
