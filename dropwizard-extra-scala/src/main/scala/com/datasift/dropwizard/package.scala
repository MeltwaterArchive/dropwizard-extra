package com.datasift

import com.yammer.dropwizard.util.{Size, Duration}


/** Scala integrations for Dropwizard */
package object dropwizard {

  implicit def quantities(n: Long): Quantities = new Quantities(n)
}

class Quantities(n: Long) {

  // Duration
  def nanoseconds: Duration = Duration.nanoseconds(n)
  def microseconds: Duration = Duration.microseconds(n)
  def milliseconds: Duration = Duration.milliseconds(n)
  def seconds: Duration = Duration.seconds(n)
  def minutes: Duration = Duration.minutes(n)
  def hours: Duration = Duration.hours(n)
  def days: Duration = Duration.days(n)

  // Size
  def bytes: Size = Size.bytes(n)
  def kilobytes: Size = Size.kilobytes(n)
  def KB: Size = kilobytes
  def KiB: Size = kilobytes
  def megabytes: Size = Size.megabytes(n)
  def MB: Size = megabytes
  def MiB: Size = megabytes
  def gigabytes: Size = Size.gigabytes(n)
  def GB: Size = gigabytes
  def GiB: Size = gigabytes
  def terabytes: Size = Size.terabytes(n)
  def TB: Size = terabytes
  def TiB: Size = terabytes
}
