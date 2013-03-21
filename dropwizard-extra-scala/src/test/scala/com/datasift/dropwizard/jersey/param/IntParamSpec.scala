package com.datasift.dropwizard.jersey.param

import org.junit.runner.RunWith
import org.specs2.mutable._
import org.specs2.runner.JUnitRunner
import javax.ws.rs.WebApplicationException

/**
 * TODO: Document
 */
@RunWith(classOf[JUnitRunner])
class IntParamSpec extends Specification {

  "A valid Int parameter" should {
    val param = IntParam("40")

    "has a Int value" in {
      param.value must_== 40L
    }
  }

  "An invalid Int parameter" should {

    "throws a WebApplicationException with an error message" in {
      IntParam("woop") must throwA[WebApplicationException].like {
        case e: WebApplicationException => {
          e.getResponse.getStatus must_== 400
          e.getResponse.getEntity must_== "Invalid parameter: woop (Must be an integer value.)"
        }
      }
    }
  }
}
