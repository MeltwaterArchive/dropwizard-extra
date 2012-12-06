package com.datasift.dropwizard

import org.skife.jdbi.v2.DBI

/**
 * Global definitions and implicits for Dropwizard DB.
 */
package object jdbi {

  /**
   * Implicit wrapper for a [[org.skife.jdbi.v2.DBI]].
   */
  implicit def enrichDatabase(db: DBI): DBIWrapper = new DBIWrapper(db)
}
