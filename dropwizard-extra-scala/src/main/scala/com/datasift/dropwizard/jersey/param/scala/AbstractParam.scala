package com.datasift.dropwizard.jersey.param.scala

import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.Response

/**
 * TODO: Document
 */
abstract class AbstractParam[A](val input: String) {

  val value: A = try {
    parse(input)
  } catch {
    case e: Exception => throw new WebApplicationException(onError(input, e))
  }

  protected def parse(input: String): A

  protected def onError(input: String, e: Throwable): Response = {
    Response.status(status).entity(renderError(input, e)).build
  }

  protected def status: Response.Status = Response.Status.BAD_REQUEST

  protected def renderError(input: String, e: Throwable): String = {
    "Invalid parameter: %s (%s)".format(input, e.getMessage)
  }

  override def toString = value.toString

}
