package com.datasift.dropwizard.jersey.inject.scala

import collection.generic.CanBuildFrom
import com.sun.jersey.server.impl.model.parameter.multivalued.MultivaluedParameterExtractor
import javax.ws.rs.core.MultivaluedMap

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

/** A parameter extractor for Scala collections of Strings.
  *
  * @tparam CC type of the collection to extract.
  * @param parameter the name of the parameter to extract the collection for.
  * @param defaultValue the default value of the collection for when the parameter does not exist.
  * @param bf the implicit builder for the collection type.
  *
  * @see [[com.sun.jersey.server.impl.model.parameter.multivalued.MultivaluedParameterExtractor]]
  */
class CollectionParameterExtractor[+CC <: Iterable[String]](parameter: String, defaultValue: String)
                                                 (implicit bf: CanBuildFrom[Nothing, String, CC])
  extends MultivaluedParameterExtractor {

  private val default = Option(defaultValue)

  def getName = parameter

  def getDefaultStringValue = defaultValue

  def extract(parameters: MultivaluedMap[String, String]): CC = {

    val builder = bf()

    val params = Option(parameters.get(parameter)) map (_.asScala) getOrElse default.toList
    builder.sizeHint(params.size)
    builder ++= params

    builder.result()
  }
}
