package com.datasift.dropwizard

import bundles.ScalaBundle
import com.codahale.dropwizard.{Application, Configuration}
import com.codahale.dropwizard.setup.Bootstrap

/** Base class for Dropwizard Services built in Scala. */
trait ScalaApplication[A <: Configuration] extends Application[A] {

  /** Entry point for this Dropwizard [[com.codahale.dropwizard.Application]].
    *
    * @param args the command-line arguments the program was invoked with.
    */
  final def main(args: Array[String]) {
    run(args)
  }

  /** Service initialization.
    *
    * Ensures that [[com.datasift.dropwizard.bundles.ScalaBundle]] is always included in Scala
    * services.
    *
    * To customize initialization behaviour, override `ScalaService#init(Bootstrap)`.
    *
    * @param bootstrap Service Bootstrap environment.
    */
  override final def initialize(bootstrap: Bootstrap[A]) {
    bootstrap.addBundle(new ScalaBundle)
    init(bootstrap)
  }

  /** Service initialization.
    *
    * @param bootstrap Service Bootstrap environment.
    */
  def init(bootstrap: Bootstrap[A]) {
    // do nothing extra by default, override to add additional initialization behavior
  }
}
