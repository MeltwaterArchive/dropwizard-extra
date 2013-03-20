package com.datasift.dropwizard

import bundles.ScalaBundle
import com.yammer.dropwizard.Service
import com.yammer.dropwizard.config.{Bootstrap, Configuration}

/**
 * Base class for Dropwizard Services built in Scala.
 */
abstract class ScalaService[A <: Configuration](name: String) extends Service[A] {

  def this() {
    this(getClass.getSimpleName)
  }

  final def main(args: Array[String]) {
    run(args)
  }

  /**
   * Ensures that the [[com.datasift.dropwizard.bundles.ScalaBundle]] is always included.
   *
   * @param bootstrap the Service Bootstrap environment
   */
  override final def initialize(bootstrap: Bootstrap[A]) {
    bootstrap.setName(name)
    bootstrap.addBundle(new ScalaBundle)
    init(bootstrap)
  }

  def init(bootstrap: Bootstrap[A]) {
    // do nothing extra by default, override to add additional initialization behavior
  }
}
