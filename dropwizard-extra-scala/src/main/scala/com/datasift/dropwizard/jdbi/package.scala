package com.datasift.dropwizard

/**
 * Global definitions and implicits for Dropwizard DB.
 */
package object jdbi {

  /**
   * Implicit wrapper for a [[org.skife.jdbi.v2.DBI]].
   */
  implicit def enrich(db: org.skife.jdbi.v2.DBI): DBI = new DBI(db)
}
