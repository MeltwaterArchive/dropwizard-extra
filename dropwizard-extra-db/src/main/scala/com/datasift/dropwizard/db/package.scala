package com.datasift.dropwizard

import com.yammer.dropwizard.db.Database

package object db {

  /** implicit wrapper for a [[com.yammer.dropwizard.db.Database]] */
  implicit def wrapDatabase(db: Database): DatabaseWrapper = {
    new DatabaseWrapper(db)
  }
}
