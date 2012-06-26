package com.datasift.dropwizard.hbase.config

import reflect.BeanProperty
import org.hibernate.validator.constraints.NotEmpty
import com.yammer.dropwizard.config.Configuration

/**[[com.yammer.dropwizard.config.Configuration]] for an HBase table */
class HBaseTableConfiguration extends Configuration {

  @BeanProperty
  @NotEmpty
  val table = ""

  @BeanProperty
  @NotEmpty
  val family = ""
}
