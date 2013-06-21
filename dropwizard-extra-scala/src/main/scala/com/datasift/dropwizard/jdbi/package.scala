package com.datasift.dropwizard

import org.skife.jdbi.v2.sqlobject.mixins.Transactional
import org.skife.jdbi.v2.Handle

/** Global definitions and implicits for JDBI. */
package object jdbi {

  /** Implicit wrapper for a [[org.skife.jdbi.v2.DBI]]. */
  implicit def enrich(db: org.skife.jdbi.v2.DBI): DBIWrapper = new DBIWrapper(db)

  /** Implicit wrapper for a [[org.skife.jdbi.v2.sqlobject.mixins.Transactional]]. */
  implicit def enrichTransactional[A <: Transactional[A]]
    (transactional: A): TransactionalWrapper[A] = new TransactionalWrapper[A](transactional)

  /** Implicit wrapper for a [[org.skife.jdbi.v2.Handle]]. */
  implicit def enrichHandle(handle: Handle): HandleWrapper = new HandleWrapper(handle)
}
