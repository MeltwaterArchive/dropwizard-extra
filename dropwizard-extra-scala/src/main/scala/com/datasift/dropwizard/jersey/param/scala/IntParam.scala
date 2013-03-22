package com.datasift.dropwizard.jersey.param.scala


object IntParam {
  def apply(value: Int): IntParam = IntParam(value.toString)
}

/**
 * TODO: Document
 */
case class IntParam(s: String) extends AbstractParam[Int](s) {
  protected def parse(input: String) = s.toInt

  override protected def renderError(input: String, e: Throwable) = {
    "Invalid parameter: %s (Must be an integer value.)".format(input)
  }
}
