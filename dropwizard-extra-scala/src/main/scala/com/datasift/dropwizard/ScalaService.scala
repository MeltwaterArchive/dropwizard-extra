package com.datasift.dropwizard

import bundles.ScalaBundle
import com.codahale.dropwizard.{Service, Configuration}
import com.codahale.dropwizard.setup.Bootstrap

/**
 * Base class for Dropwizard Services built in Scala.
 */
abstract class ScalaService[A <: Configuration] extends Service[A] {

  final def main(args: Array[String]) {
    run(args)
  }

  /**
   * Ensures that the [[com.datasift.dropwizard.bundles.ScalaBundle]] is always included.
   *
   * @param bootstrap the Service Bootstrap environment
   */
  override final def initialize(bootstrap: Bootstrap[A]) {
    bootstrap.addBundle(new ScalaBundle)
    init(bootstrap)
  }

  def init(bootstrap: Bootstrap[A]) {
    // do nothing extra by default, override to add additional initialization behavior
  }
}
