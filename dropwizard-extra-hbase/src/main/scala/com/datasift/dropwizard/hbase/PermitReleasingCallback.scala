package com.datasift.dropwizard.hbase

import java.util.concurrent.Semaphore
import com.stumbleupon.async.Callback

/** [[com.stumbleupon.async.Callback]] that releases a permit from a [[java.util.concurrent.Semaphore]]
 *
 * Any argument provided to the callback will be returned verbatim once the
 * permit has been released
 *
 * @tparam A return type for the callback, passed-through as-is
 * @param semaphore Semaphore to release the permit back to
 */
private[hbase] class PermitReleasingCallback[A](semaphore: Semaphore) extends Callback[A, A] {

  def call(arg: A): A = {
    semaphore.release()
    arg
  }
}
