package com.datasift.dropwizard.jersey.inject.scala

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.mutable._
import org.specs2.mock.Mockito
import com.sun.jersey.api.core.{ExtendedUriInfo, HttpContext}
import com.sun.jersey.core.util.MultivaluedMapImpl
import com.sun.jersey.core.spi.component.{ComponentScope, ComponentContext}
import javax.ws.rs.QueryParam
import com.sun.jersey.api.model.Parameter

/** Tests for
  * [[com.datasift.dropwizard.jersey.inject.scala.CollectionsQueryParamInjectableProvider]].
  */
@RunWith(classOf[JUnitRunner])
class CollectionsQueryParamInjectableProviderSpec extends Specification with Mockito {

  "ScalaCollectionsQueryParamInjectableProvider" should {

    val httpContext = mock[HttpContext]
    val uriInfo = mock[ExtendedUriInfo]
    val params = new MultivaluedMapImpl()
    params.add("name", "one")
    params.add("name", "two")
    params.add("name", "three")

    httpContext.getUriInfo returns uriInfo
    uriInfo.getQueryParameters(any) returns params

    val context = mock[ComponentContext]
    val queryParam = mock[QueryParam]

    val provider = new CollectionsQueryParamInjectableProvider

    "have a per-request scope" in {
      provider.getScope must be(ComponentScope.PerRequest)
    }

    "return an injectable for Seq instances" in {
      val param = new Parameter(Array(), null, null, "name", null, classOf[Seq[String]], false, "default")
      val injectable = provider.getInjectable(context, queryParam, param)

      injectable.getValue(httpContext) must_== Seq("one", "two", "three")
    }

    "return an injectable for List instances" in {
      val param = new Parameter(Array(), null, null, "name", null, classOf[List[String]], false, "default")
      val injectable = provider.getInjectable(context, queryParam, param)

      injectable.getValue(httpContext) must_== List("one", "two", "three")
    }

    "return an injectable for Vector instances" in {
      val param = new Parameter(Array(), null, null, "name", null, classOf[Vector[String]], false, "default")
      val injectable = provider.getInjectable(context, queryParam, param)

      injectable.getValue(httpContext) must_== Vector("one", "two", "three")
    }

    "return an injectable for IndexedSeq instances" in {
      val param = new Parameter(Array(), null, null, "name", null, classOf[IndexedSeq[String]], false, "default")
      val injectable = provider.getInjectable(context, queryParam, param)

      injectable.getValue(httpContext) must_== IndexedSeq("one", "two", "three")
    }

    "return an injectable for Set instances" in {
      val param = new Parameter(Array(), null, null, "name", null, classOf[Set[String]], false, "default")
      val injectable = provider.getInjectable(context, queryParam, param)

      injectable.getValue(httpContext) must_== Set("one", "two", "three")
    }

    "return an injectable for Option instances" in {
      val param = new Parameter(Array(), null, null, "name", null, classOf[Option[String]], false, "default")
      val injectable = provider.getInjectable(context, queryParam, param)

      injectable.getValue(httpContext) must_== Option("one")
    }
  }
}
