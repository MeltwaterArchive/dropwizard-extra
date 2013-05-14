package com.datasift.dropwizard.jersey.param.scala

/** Factory object for [[com.datasift.dropwizard.jersey.param.scala.IntParam]]. */
object IntParam {

  /** Creates a parameter extractor for the given value. */
  def apply(value: Int): AbstractParam[Int] = IntParam(value.toString)
}

/** Parameter extractor for [[scala.Int]].
  *
  * @param s the input data to extract the [[scala.Int]] from.
  *
  * @see [[com.datasift.dropwizard.jersey.param.scala.AbstractParam]]
  */
case class IntParam(s: String) extends AbstractParam[Int](s) {

  protected def parse(input: String) = s.toInt

  override protected def renderError(input: String, e: Throwable) = {
    "Invalid parameter: %s (Must be an integer value.)".format(input)
  }
}
