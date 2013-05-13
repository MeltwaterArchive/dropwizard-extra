package com.datasift.dropwizard

/** Global definitions and implicits for JDBI. */
package object jdbi {

  /** Implicit wrapper for a [[org.skife.jdbi.v2.DBI]]. */
  implicit def enrich(db: org.skife.jdbi.v2.DBI): DBI = new DBI(db)

  /** Implicit wrapper for a [[org.skife.jdbi.v2.sqlobject.mixins.Transactional]]. */
  implicit def enrichTransactional[A <: org.skife.jdbi.v2.sqlobject.mixins.Transactional[A]]
    (transactional: A): Transactional[A] = new Transactional[A](transactional)
}
