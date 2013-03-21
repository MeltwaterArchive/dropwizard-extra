package com.datasift.dropwizard.jersey.inject.scala

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.mutable._
import org.specs2.mock.Mockito
import com.sun.jersey.core.util.MultivaluedMapImpl

/**
 * TODO: Document
 */
@RunWith(classOf[JUnitRunner])
class OptionParameterExtractorSpec extends Specification with Mockito {

  "Extracting a parameter" should {

    val extractor = new OptionParameterExtractor("name", "default")

    "have a name" in {
      extractor.getName must be("name")
    }

    "have a default value" in {
      extractor.getDefaultStringValue must be("default")
    }

    "extract the first value from a set of parameter values" in {
      val params = new MultivaluedMapImpl()
      params.add("name", "one")
      params.add("name", "two")
      params.add("name", "three")

      extractor.extract(params) must beSome("one")
    }

    "uses the default value if no parameter exists" in {
      val params = new MultivaluedMapImpl()

      extractor.extract(params) must beSome("default")
    }
  }

  "Extracting a parameter without a default value" should {

    val extractor = new OptionParameterExtractor("name", null)

    "return None if no parameter exists" in {
      extractor.extract(new MultivaluedMapImpl()) must beNone
    }
  }
}
