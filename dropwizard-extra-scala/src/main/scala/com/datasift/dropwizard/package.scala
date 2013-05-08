package com.datasift

import com.codahale.dropwizard.util.{Size, Duration}

/** Implicit Scala integrations for miscellaneous Dropwizard utilities */
package object dropwizard {

  /** Implicit wrapper for [[com.codahale.dropwizard.util.Size]] and
    * [[com.codahale.dropwizard.util.Duration]] quantities.
    *
    * @param n the quantity to wrap.
    */
  implicit def quantities(n: Long): Quantities = new Quantities(n)
}

/** Wrapper for [[com.codahale.dropwizard.util.Size]] and
  * [[com.codahale.dropwizard.util.Duration]] quantities.
  *
  * @param n the quantity to wrap.
  */
class Quantities(n: Long) {

  /** Duration in nanoseconds. */
  def nanoseconds: Duration = Duration.nanoseconds(n)

  /** Duration in microseconds. */
  def microseconds: Duration = Duration.microseconds(n)

  /** Duration in milliseconds. */
  def milliseconds: Duration = Duration.milliseconds(n)

  /** Duration in seconds. */
  def seconds: Duration = Duration.seconds(n)

  /** Duration in minutes. */
  def minutes: Duration = Duration.minutes(n)

  /** Duration in hours. */
  def hours: Duration = Duration.hours(n)

  /** Duration in days. */
  def days: Duration = Duration.days(n)

  /** Size in bytes */
  def bytes: Size = Size.bytes(n)

  /** Size in kilobytes */
  def kilobytes: Size = Size.kilobytes(n)

  /** Size in kilobytes */
  def KB: Size = kilobytes

  /** Size in kilobytes */
  def KiB: Size = kilobytes

  /** Size in megabytes */
  def megabytes: Size = Size.megabytes(n)

  /** Size in megabytes */
  def MB: Size = megabytes

  /** Size in megabytes */
  def MiB: Size = megabytes

  /** Size in gigabytes */
  def gigabytes: Size = Size.gigabytes(n)

  /** Size in gigabytes */
  def GB: Size = gigabytes

  /** Size in gigabytes */
  def GiB: Size = gigabytes

  /** Size in terabytes */
  def terabytes: Size = Size.terabytes(n)

  /** Size in terabytes */
  def TB: Size = terabytes

  /** Size in terabytes */
  def TiB: Size = terabytes
}
