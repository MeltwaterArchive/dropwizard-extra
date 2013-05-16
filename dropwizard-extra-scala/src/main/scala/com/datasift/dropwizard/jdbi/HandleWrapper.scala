package com.datasift.dropwizard.jdbi

import org.skife.jdbi.v2.{TransactionIsolationLevel, TransactionStatus, TransactionCallback, Handle}

/** Provides idiomatic Scala enhancements to the JDBI API.
  *
  * @param handle the [[org.skife.jdbi.v2.Handle]] instance to wrap.
  */
class HandleWrapper(handle: Handle) {

  /** Creates a typed DAO instance attached to this [[org.skife.jdbi.v2.Handle]].
    *
    * @tparam A type of the DAO to create.
    * @return a DAO instance for the specified type.
    */
  def attach[A : Manifest]: A = {
    handle.attach(implicitly[Manifest[A]].erasure.asInstanceOf[Class[A]])
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
    handle.inTransaction(new TransactionCallback[A] {
      def inTransaction(conn: Handle, status: TransactionStatus): A = f(conn)
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
  def inTransaction[A](f: (Handle, TransactionStatus) => A): A = {
    handle.inTransaction(new TransactionCallback[A] {
      def inTransaction(conn: Handle, status: TransactionStatus): A = f(conn, status)
    })
  }

  /** Executes the given function within a transaction.
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
    handle.inTransaction(isolation, new TransactionCallback[A] {
      def inTransaction(conn: Handle, status: TransactionStatus): A = f(conn)
    })
  }

  /** Executes the given function within a transaction.
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
    handle.inTransaction(isolation, new TransactionCallback[A] {
      def inTransaction(conn: Handle, status: TransactionStatus): A = f(conn, status)
    })
  }
}
