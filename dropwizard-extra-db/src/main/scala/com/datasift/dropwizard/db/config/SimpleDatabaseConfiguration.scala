package com.datasift.dropwizard.db.config

import com.yammer.dropwizard.config.Configuration
import com.yammer.dropwizard.db.DatabaseConfiguration
import javax.validation.Valid
import javax.validation.constraints.NotNull
import reflect.BeanProperty

/**[[com.yammer.dropwizard.config.Configuration]] mix-in for a single database connection */
trait SimpleDatabaseConfiguration {
  self: Configuration =>

  @BeanProperty
  @Valid
  @NotNull
  val database = new DatabaseConfiguration
}
