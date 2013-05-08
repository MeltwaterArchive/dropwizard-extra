package com.datasift.dropwizard.jersey.param.scala

import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.Response

/** Base for Scala parameter extractors.
  *
  * @tparam A the type of the parameter to extract.
  * @param input the raw data to extract the parameter from.
  */
abstract class AbstractParam[A](val input: String) {

  /** The extracted parameter.
    *
    * @throws WebApplicationException if a value of the correct type cannot be extracted.
    */
  val value: A = try {
    parse(input)
  } catch {
    case e: Exception => throw new WebApplicationException(onError(input, e))
  }

  /** Extracts the parameter from the given input data.
    *
    * @param input the data to extract the parameter from.
    * @return the extracted parameter.
    * @throws Exception if a value of the correct type cannot be extracted.
    */
  protected def parse(input: String): A

  /** Creates a response for the given error.
    *
    * @param input the input data that caused the error.
    * @param e the error to create a response for.
    * @return a [[javax.ws.rs.core.Response]] for the given error.
    */
  protected def onError(input: String, e: Throwable): Response = {
    Response.status(status).entity(renderError(input, e)).build
  }

  /** The HTTP Status code to use for error responses when the parameter cannot be extracted. */
  protected def status: Response.Status = Response.Status.BAD_REQUEST

  /** Renders an error message for the given error.
    *
    * @param input the input data that caused the error.
    * @param e the error to render an error message for.
    * @return a human-readable message describing the error.
    */
  protected def renderError(input: String, e: Throwable): String = {
    "Invalid parameter: %s (%s)".format(input, e.getMessage)
  }

  /** The [[java.lang.String]] value of this parameter. */
  override def toString = value.toString

}
