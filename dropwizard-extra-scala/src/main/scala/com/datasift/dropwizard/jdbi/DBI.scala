package com.datasift.dropwizard.jdbi

import com.codahale.dropwizard.db.DataSourceFactory
import com.codahale.dropwizard.setup.Environment
import com.codahale.dropwizard.jdbi.DBIFactory
import org.skife.jdbi.v2.{TransactionIsolationLevel, TransactionCallback, TransactionStatus, Handle}
import org.skife.jdbi.v2.tweak.HandleCallback
import scala.{IterableContainerFactory, OptionArgumentFactory, OptionContainerFactory}

/** Factory object for [[org.skife.jdbi.v2.DBI]] instances. */
object DBI {

  /** Creates a [[org.skife.jdbi.v2.DBI]] from the given configuration.
    *
    * The name of this instance will be the JDBC URL of the database.
    *
    * @param env environment to manage the database connection lifecycle.
    * @param conf configuration for the database connection.
    * @return a configured and managed [[org.skife.jdbi.v2.DBI]] instance.
    */
  def apply(env: Environment, conf: DataSourceFactory): org.skife.jdbi.v2.DBI = {
    apply(env, conf, conf.getUrl)
  }

  /** Creates a [[org.skife.jdbi.v2.DBI]] from the given configuration.
    *
    * @param env environment to manage the database connection lifecycle.
    * @param conf configuration for the database connection.
    * @param name the name of this DBI instance.
    * @return a configured and managed [[org.skife.jdbi.v2.DBI]] instance.
    */
  def apply(env: Environment, conf: DataSourceFactory, name: String): org.skife.jdbi.v2.DBI = {
    val dbi = new DBIFactory().build(env, conf, name)

    // register scala type factories
    dbi.registerArgumentFactory(new OptionArgumentFactory(conf.getDriverClass))
    dbi.registerContainerFactory(new OptionContainerFactory)
    dbi.registerContainerFactory(new IterableContainerFactory[Seq])
    dbi.registerContainerFactory(new IterableContainerFactory[Set])

    dbi
  }
}

/** Provides idiomatic Scala enhancements to the JDBI API.
  *
  * @param db the [[org.skife.jdbi.v2.DBI]] instance to wrap.
  */
class DBIWrapper(db: org.skife.jdbi.v2.DBI) {

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

  /** Applies the given function with a DBI [[org.skife.jdbi.v2.Handle]].
    *
    * @tparam A the return type of the function to apply.
    * @param f the function to apply the handle to.
    * @return the result of applying the function.
    * @throws Exception if an Exception is thrown by the function.
    */
  def withHandle[A](f: Handle => A): A = {
    db.withHandle(new HandleCallback[A] {
      def withHandle(handle: Handle): A = f(handle)
    })
  }
}
