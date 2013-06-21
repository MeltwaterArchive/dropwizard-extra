package com.datasift.dropwizard.jersey.inject.scala

import com.sun.jersey.spi.inject.InjectableProvider
import javax.ws.rs.QueryParam
import com.sun.jersey.api.model.Parameter
import com.sun.jersey.core.spi.component.{ComponentScope, ComponentContext}
import javax.ws.rs.ext.Provider

/** Provides query parameter extractors for each type of Scala collection.
  *
  * @see [[com.datasift.dropwizard.jersey.inject.scala.CollectionQueryParamInjectable]]
  * @see [[com.datasift.dropwizard.jersey.inject.scala.CollectionParameterExtractor]]
  * @see [[com.datasift.dropwizard.jersey.inject.scala.OptionParameterExtractor]]
  */
@Provider
class CollectionsQueryParamInjectableProvider
  extends InjectableProvider[QueryParam, Parameter] {

  def getScope = ComponentScope.PerRequest

  def getInjectable(context: ComponentContext, queryParam: QueryParam, param: Parameter) = {
    val name = param.getSourceName
    val default = param.getDefaultValue
    val clazz = param.getParameterClass

    if (name != null && !name.isEmpty) {
      val ex = if (clazz == classOf[Seq[String]]) {
          new CollectionParameterExtractor[Seq[String]](name, default)
        } else if (clazz == classOf[List[String]]) {
          new CollectionParameterExtractor[List[String]](name, default)
        } else if (clazz == classOf[Vector[String]]) {
          new CollectionParameterExtractor[Vector[String]](name, default)
        } else if (clazz == classOf[IndexedSeq[String]]) {
          new CollectionParameterExtractor[IndexedSeq[String]](name, default)
        } else if (clazz == classOf[Set[String]]) {
          new CollectionParameterExtractor[Set[String]](name, default)
        } else if (clazz == classOf[Option[String]]) {
          new OptionParameterExtractor(name, default)
        } else {
          null
        }

      if (ex != null) {
        new CollectionQueryParamInjectable(ex, !param.isEncoded)
      } else {
        null
      }
    } else {
      null
    }
  }
}
