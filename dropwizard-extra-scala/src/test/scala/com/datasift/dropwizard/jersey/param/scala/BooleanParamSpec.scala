package com.datasift.dropwizard.jersey.param.scala

import org.junit.runner.RunWith
import org.specs2.mutable._
import org.specs2.runner.JUnitRunner
import javax.ws.rs.WebApplicationException

/** Tests for [[com.datasift.dropwizard.jersey.param.scala.BooleanParam]]. */
@RunWith(classOf[JUnitRunner])
class BooleanParamSpec extends Specification {

  "A valid Boolean parameter" should {
    val param = BooleanParam("true")

    "has a Boolean value" in {
      param.value must beTrue
    }
  }

  "An invalid Boolean parameter" should {

    "throws a WebApplicationException with an error message" in {
      BooleanParam("woop") must throwA[WebApplicationException].like {
        case e: WebApplicationException => {
          e.getResponse.getStatus must_== 400
          e.getResponse.getEntity must_== "Invalid parameter: woop (Must be \"true\" or \"false\".)"
        }
      }
    }
  }
}
