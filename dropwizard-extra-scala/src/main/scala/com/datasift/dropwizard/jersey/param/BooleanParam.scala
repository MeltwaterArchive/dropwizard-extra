package com.datasift.dropwizard.jersey.param

object BooleanParam {
  def apply(value: Boolean): BooleanParam = BooleanParam(value.toString)
}

/**
 * TODO: Document
 */
case class BooleanParam(s: String) extends AbstractParam[Boolean](s) {
  protected def parse(input: String) = input.toBoolean

  override protected def renderError(input: String, e: Throwable) = {
    "Invalid parameter: %s (Must be \"true\" or \"false\".)".format(input)
  }
}
