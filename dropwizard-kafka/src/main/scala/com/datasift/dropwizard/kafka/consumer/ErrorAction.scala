package com.datasift.dropwizard.kafka.consumer

import com.yammer.dropwizard.util.Duration
import org.codehaus.jackson.annotate.JsonCreator

/** factory for ErrorActions */
object ErrorAction {

  /** parse the specified String in to an ErrorAction or die trying */
  @JsonCreator
  def apply(action: String): ErrorAction = {
    Restart(action) orElse
    RestartAfter(action) orElse
    Shutdown(action) orElse
    ShutdownAfter(action) getOrElse {
      throw new IllegalArgumentException(
        "invalid error action '%s'".format(action))
    }
  }
}

/** base type for ErrorActions */
sealed trait ErrorAction

/** base type for ErrorActions parameterized by a delay */
sealed trait DelayedErrorAction extends ErrorAction {
  def delay: Duration
}

/** abstraction for factory objects that create an ErrorAction instance */
sealed abstract class ErrorActionFactory(val labels: String*)

/** mix-in for singleton instances of an ErrorAction */
sealed trait SimpleErrorActionFactory {
  self: ErrorActionFactory with ErrorAction =>

  def apply(input: String): Option[ErrorAction] = {
    labels collectFirst {
      case label if input.trim.toLowerCase == label => this
    }
  }
}

/** mix-in for factory objects that create an ErrorAction parameterized by a delay */
sealed trait DelayedErrorActionFactory[A <: DelayedErrorAction] {
  self: ErrorActionFactory =>

  def apply(duration: Duration): A

  def apply(input: String): Option[ErrorAction] = {
    labels collectFirst {
      case label if input.startsWith(label) =>
        apply(Duration.parse(input.substring(label.length).trim))
    }
  }
}

/** ErrorAction for restarting a failed consumer thread after a delay */
case class RestartAfter(delay: Duration) extends DelayedErrorAction
object RestartAfter
  extends ErrorActionFactory("restart after")
  with DelayedErrorActionFactory[RestartAfter]

/** ErrorAction for shutting down the entire consumer after a delay */
case class ShutdownAfter(delay: Duration) extends DelayedErrorAction
object ShutdownAfter
  extends ErrorActionFactory("shutdown after", "stop after")
  with DelayedErrorActionFactory[ShutdownAfter]

/** ErrorAction for restarting a failed consumer thread immediately */
case object Restart
  extends ErrorActionFactory("restart")
  with SimpleErrorActionFactory
  with ErrorAction

/** ErrorAction for shutting down the entire consumer immediately */
case object Shutdown
  extends ErrorActionFactory("shutdown", "stop")
  with SimpleErrorActionFactory
  with ErrorAction
