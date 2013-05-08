package com.datasift.dropwizard.jersey.inject.scala

import com.sun.jersey.server.impl.model.parameter.multivalued.{MultivaluedParameterExtractor, ExtractorContainerException}
import com.sun.jersey.server.impl.inject.AbstractHttpContextInjectable
import com.sun.jersey.api.core.HttpContext
import com.sun.jersey.api.ParamException

/** A query parameter extractor for Scala collections.
  *
  * @param extractor the extractor for the collection type to extract.
  * @param decode whether to decode sequences of escaped octets in the parameter value.
  */
class CollectionQueryParamInjectable(extractor: MultivaluedParameterExtractor, decode: Boolean)
  extends AbstractHttpContextInjectable[Object] {

  def getValue(context: HttpContext) = try {
    extractor.extract(context.getUriInfo.getQueryParameters(decode))
  } catch {
    case e: ExtractorContainerException =>
      throw new ParamException.QueryParamException(
        e.getCause, extractor.getName, extractor.getDefaultStringValue)
  }
}
