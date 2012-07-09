package com.datasift.dropwizard

import com.yammer.dropwizard.db.Database

/**
 * Global definitions and implicits for Dropwizard DB.
 */
package object db {

  /**
   * Implicit wrapper for a [[com.yammer.dropwizard.db.Database]].
   */
  implicit def enrichDatabase(db: Database): DatabaseWrapper = new DatabaseWrapper(db)
}
