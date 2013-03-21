package com.datasift.dropwizard.inject.scala

import org.specs2.mutable._
import org.specs2.mock.Mockito
import com.sun.jersey.server.impl.model.parameter.multivalued.MultivaluedParameterExtractor
import com.sun.jersey.api.core.{ExtendedUriInfo, HttpContext}
import javax.ws.rs.core.MultivaluedMap
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

/**
 * Tests [[com.datasift.dropwizard.inject.scala.CollectionQueryParamInjectable]]
 */
@RunWith(classOf[JUnitRunner])
class CollectionQueryParamInjectableSpec extends Specification with Mockito {

  val extractor = mock[MultivaluedParameterExtractor]
  val context = mock[HttpContext]
  val uriInfo = mock[ExtendedUriInfo]
  val params = mock[MultivaluedMap[String, String]]
  val extracted = mock[Object]

  extractor.extract(params) returns extracted
  context.getUriInfo returns uriInfo

  "A decoding ScalaCollectionQueryParamInjectable" should {

    val injectable = new CollectionQueryParamInjectable(extractor, true)
    uriInfo.getQueryParameters(true) returns params

    "extract query parameters" in {
      injectable.getValue(context) must be(extracted)
    }
  }

  "A non-decoding ScalaCollectionQueryParamInjectable" should {

    val injectable = new CollectionQueryParamInjectable(extractor, false)
    uriInfo.getQueryParameters(false) returns params

    "extract query parameters" in {
      injectable.getValue(context) must be(extracted)
    }
  }

}
