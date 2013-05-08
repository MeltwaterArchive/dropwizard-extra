package com.datasift.dropwizard.jersey.inject.scala

import com.sun.jersey.server.impl.model.parameter.multivalued.MultivaluedParameterExtractor
import javax.ws.rs.core.MultivaluedMap

/** A parameter extractor for a String [[scala.Option]].
  *
  * @param parameter the name of the optional String parameter.
  * @param defaultValue the default value if the parameter did not exist.
  */
class OptionParameterExtractor(parameter: String, defaultValue: String)
  extends MultivaluedParameterExtractor {

  private val default = Option(defaultValue)

  def getName = parameter

  def getDefaultStringValue = defaultValue

  def extract(parameters: MultivaluedMap[String, String]) = {
    Option(parameters.getFirst(parameter)) orElse default
  }
}
