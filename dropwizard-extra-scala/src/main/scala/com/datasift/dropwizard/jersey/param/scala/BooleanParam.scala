package com.datasift.dropwizard.jersey.param.scala

/** Factory object for [[com.datasift.dropwizard.jersey.param.scala.BooleanParam]]. */
object BooleanParam {

  /** Creates a parameter extractor for the given value. */
  def apply(value: Boolean): AbstractParam[Boolean] = BooleanParam(value.toString)
}

/** Parameter extractor for [[scala.Boolean]].
  *
  * @param s the input data to extract the [[scala.Boolean]] from.
  *
  * @see [[com.datasift.dropwizard.jersey.param.scala.AbstractParam]]
  */
case class BooleanParam(s: String) extends AbstractParam[Boolean](s) {

  protected def parse(input: String) = input.toBoolean

  override protected def renderError(input: String, e: Throwable) = {
    "Invalid parameter: %s (Must be \"true\" or \"false\".)".format(input)
  }
}
