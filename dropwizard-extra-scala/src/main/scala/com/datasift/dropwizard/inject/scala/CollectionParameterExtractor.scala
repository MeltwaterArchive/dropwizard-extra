package com.datasift.dropwizard.inject.scala

import collection.generic.CanBuildFrom
import com.sun.jersey.server.impl.model.parameter.multivalued.MultivaluedParameterExtractor
import javax.ws.rs.core.MultivaluedMap

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

/**
 * TODO: Document
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
