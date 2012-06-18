package com.datasift.dropwizard.hbase.config

import reflect.BeanProperty
import org.hibernate.validator.constraints.NotEmpty

/**[[com.yammer.dropwizard.config.Configuration]] for an HBase table */
class HBaseTableConfiguration(defaultTable: String = null, defaultFamily: String = null) {

  @BeanProperty
  @NotEmpty
  val table = defaultTable

  @BeanProperty
  @NotEmpty
  val family = defaultFamily
}
