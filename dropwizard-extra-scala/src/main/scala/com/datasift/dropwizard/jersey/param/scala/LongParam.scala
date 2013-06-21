package com.datasift.dropwizard.jersey.param.scala

/** Factory object for [[com.datasift.dropwizard.jersey.param.scala.LongParam]]. */
object LongParam {

  /** Creates a parameter extractor for the given value. */
  def apply(value: Long): AbstractParam[Long] = LongParam(value.toString)
}

/** Parameter extractor for [[scala.Long]].
  *
  * @param s the input data to extract the [[scala.Long]] from.
  *
  * @see [[com.datasift.dropwizard.jersey.param.scala.AbstractParam]]
  */
case class LongParam(s: String) extends AbstractParam[Long](s) {
  protected def parse(input: String) = s.toLong

  override protected def renderError(input: String, e: Throwable) = {
    "Invalid parameter: %s (Must be an integer value.)".format(input)
  }
}
