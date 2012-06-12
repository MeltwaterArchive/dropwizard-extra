package com.datasift.dropwizard.hbase

import com.yammer.metrics.core.TimerContext
import com.stumbleupon.async.Callback

/** [[com.stumbleupon.async.Callback]] stops a [[com.yammer.metrics.scala.Timer]]
 *
 * Any argument provided to the callback will be returned verbatim once the
 * permit has been released
 *
 * @tparam A return type for the callback, passed-through as-is
 * @param timer Context of the timer to stop when the callback is called
 */
private[hbase] class TimerStoppingCallback[A](timer: TimerContext) extends Callback[A, A] {

  def call(arg: A): A = {
    timer.stop()
    arg
  }
}
