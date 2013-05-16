package com.datasift.dropwizard.jdbi

import org.skife.jdbi.v2.{TransactionIsolationLevel, Transaction, TransactionStatus}

/** Provides enhancements to the Dropwizard jDBI API for transactional DAOs.
  *
  * @param transactionl the [[org.skife.jdbi.v2.sqlobject.mixins.Transactional]] object to wrap.
  */
class TransactionalWrapper[A <: org.skife.jdbi.v2.sqlobject.mixins.Transactional[A]](transactionl: A) {

  /** Executes the given function within a transaction of the given isolation level.
    *
    * @tparam B the type of the result of the function being executed.
    * @param isolation the isolation level for the transaction.
    * @param f the function on this object to execute within the transaction.
    * @return the result of the function being executed.
    * @throws Exception if an Exception is thrown by the function, the transaction will be
    *                   rolled-back.
    */
  def inTransaction[B](isolation: TransactionIsolationLevel)
                      (f: A => B): B = {
    transactionl.inTransaction[B](isolation, new Transaction[B, A] {
      def inTransaction(tx: A, status: TransactionStatus): B = f(tx)
    })
  }

  /** Executes the given function within a transaction of the given isolation level.
    *
    * @tparam B the type of the result of the function being executed.
    * @param isolation the isolation level for the transaction.
    * @param f the function on this object to execute within the transaction.
    * @return the result of the function being executed.
    * @throws Exception if an Exception is thrown by the function, the transaction will be
    *                   rolled-back.
    */
  def inTransaction[B](isolation: TransactionIsolationLevel)
                      (f: (A, TransactionStatus) => B): B = {
    transactionl.inTransaction[B](isolation, new Transaction[B, A] {
      def inTransaction(tx: A, status: TransactionStatus): B = f(tx, status)
    })
  }

  /** Executes the given function within a transaction.
    *
    * @tparam B the type of the result of the function being executed.
    * @param f the function on this object to execute within the transaction.
    * @return the result of the function being executed.
    * @throws Exception if an Exception is thrown by the function, the transaction will be
    *                   rolled-back.
    */
  def inTransaction[B](f: A => B): B = {
    transactionl.inTransaction[B](new Transaction[B, A] {
      def inTransaction(tx: A, status: TransactionStatus): B = f(tx)
    })
  }


  /** Executes the given function within a transaction.
    *
    * @tparam B the type of the result of the function being executed.
    * @param f the function on this object to execute within the transaction.
    * @return the result of the function being executed.
    * @throws Exception if an Exception is thrown by the function, the transaction will be
    *                   rolled-back.
    */
  def inTransaction[B](f: (A, TransactionStatus) => B): B = {
    transactionl.inTransaction[B](new Transaction[B, A] {
      def inTransaction(tx: A, status: TransactionStatus): B = f(tx, status)
    })
  }
}
