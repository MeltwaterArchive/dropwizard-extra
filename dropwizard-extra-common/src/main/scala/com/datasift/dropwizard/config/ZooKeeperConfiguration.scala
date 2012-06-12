package com.datasift.dropwizard.config

import com.yammer.dropwizard.config.Configuration
import org.hibernate.validator.constraints.{NotEmpty, Range}
import reflect.BeanProperty
import javax.validation.constraints.{Min, NotNull}

/** Configuration for a ZooKeeper cluster */
class ZooKeeperConfiguration extends Configuration {

  /** hosts that make up the ZooKeeper quorum */
  @BeanProperty
  @NotEmpty
  val hosts = Array("localhost")

  /** port to connect to ZooKeeper nodes on */
  @BeanProperty
  @NotNull
  @Range(min = 0, max = 49151)
  val port = 2181

  /** timeout, in milliseoncds, for idle connections to a ZooKeeper node */
  @BeanProperty
  @NotNull
  @Min(0)
  val timeout = 60000

  /** Quorum specificiation as a comma-delimited String of host:port pairs. */
  def quorumSpec: String = hosts map (_ + ":" + port) mkString (",")
}
