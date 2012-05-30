package com.datasift.dropwizard.db

import com.datasift.dropwizard.ComposableService
import com.yammer.dropwizard.Logging
import com.yammer.dropwizard.config.Environment
import javax.security.auth.login.Configuration
import com.yammer.dropwizard.db.{DatabaseConfiguration, Database, DatabaseFactory}
import javax.validation.Valid
import reflect.BeanProperty
import org.hibernate.validator.constraints.NotEmpty

trait ConfigurableDatabaseConnection { self: Configuration =>

  @BeanProperty
  @Valid
  val database = new NamedDatabaseConfiguration
}

class NamedDatabaseConfiguration extends DatabaseConfiguration {

  @BeanProperty
  @NotEmpty
  val name: String = null
}

/** mix-in for a single database connection
 *
 * To connect to multiple databases, configure them manually using
 * [[com.yammer.dropwizard.db.DatabaseFactory]] and
 * [[com.yammer.dropwizard.db.DatabaseConfiguration]].
 */
trait DatabaseConnection {
  self: ComposableService[_ <: ConfigurableDatabaseConnection] with Logging =>

  /** the database connection */
  var db: Database = null

  self beforeInit { (conf: ConfigurableDatabaseConnection, env: Environment) =>
    val factory = new DatabaseFactory(env)
    db = factory.build(conf.database, conf.database.name)
  }

  implicit def wrapDatabase(db: Database): DatabaseWrapper = new DatabaseWrapper(db)

  /** wrapper for more idiomatic scala use */
  class DatabaseWrapper(db: Database) {

    /** create a DAO for a type */
    def daoFor[A : Manifest] = {
      db.onDemand[A](implicitly[Manifest[A]].erasure.asInstanceOf[Class[A]])
    }
  }
}
