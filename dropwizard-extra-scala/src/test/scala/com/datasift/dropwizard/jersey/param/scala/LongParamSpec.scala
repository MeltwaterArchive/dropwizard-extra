package com.datasift.dropwizard.jersey.param.scala

import org.junit.runner.RunWith
import org.specs2.mutable._
import org.specs2.runner.JUnitRunner
import javax.ws.rs.WebApplicationException

/** Tests for [[com.datasift.dropwizard.jersey.param.scala.LongParam]]. */
@RunWith(classOf[JUnitRunner])
class LongParamSpec extends Specification {

  "A valid Long parameter" should {
    val param = LongParam("40")

    "has a Long value" in {
      param.value must_== 40L
    }
  }

  "An invalid Long parameter" should {

    "throws a WebApplicationException with an error message" in {
      LongParam("woop") must throwA[WebApplicationException].like {
        case e: WebApplicationException => {
          e.getResponse.getStatus must_== 400
          e.getResponse.getEntity must_== "Invalid parameter: woop (Must be an integer value.)"
        }
      }
    }
  }
}
