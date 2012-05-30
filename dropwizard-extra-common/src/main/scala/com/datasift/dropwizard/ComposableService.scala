package com.datasift.dropwizard

import collection.mutable
import com.yammer.dropwizard.config.{Environment, Configuration}
import com.yammer.dropwizard.ScalaService

/** A [[com.yammer.dropwizard.ScalaService]] that's composable via traits
 *
 * To intialize your service, implement the `init` method
 *
 * Mix-ins can opt to run code before or after the `init` method is called.
 *
 * Example:
 * {{{
 *   object MyService extends ComposableService[MyConfiguration]("MyService")
 *     with Logging
 *     with GraphiteReporting {
 *
 *     def init(conf: MyConfiguration, env: Environment) {
 *       // service init code
 *     }
 *   }
 * }}}
 */
abstract class ComposableService[T <: Configuration](name: String)
  extends ScalaService[T](name) {

  private val preInitFuncs = mutable.ListBuffer.empty[(T, Environment) => Unit]
  private val postInitFuncs  = mutable.ListBuffer.empty[(T, Environment) => Unit]

  final def initialize(conf: T, env: Environment) {
    preInitFuncs foreach { _(conf, env) }
    init(conf, env)
    postInitFuncs foreach { _(conf, env) }
  }

  /** add a function to run before initialization */
  protected def beforeInit(f: (T, Environment) => Unit) {
    preInitFuncs += f
  }

  /** add a function to run after initialization */
  protected def afterInit(f: (T, Environment) => Unit) {
    postInitFuncs += f
  }

  /** service initialization */
  def init(conf: T, env: Environment): Unit
}
