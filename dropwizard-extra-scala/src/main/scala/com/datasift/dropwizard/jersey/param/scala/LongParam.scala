package com.datasift.dropwizard.jersey.param.scala


object LongParam {
  def apply(value: Long): LongParam = LongParam(value.toString)
}

/**
 * TODO: Document
 */
case class LongParam(s: String) extends AbstractParam[Long](s) {
  protected def parse(input: String) = s.toLong

  override protected def renderError(input: String, e: Throwable) = {
    "Invalid parameter: %s (Must be an integer value.)".format(input)
  }
}
