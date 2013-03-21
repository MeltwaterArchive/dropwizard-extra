package com.datasift.dropwizard.jersey.inject.scala

import org.specs2.mutable._
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import com.sun.jersey.core.util.MultivaluedMapImpl

/**
 * TODO: Document
 */
@RunWith(classOf[JUnitRunner])
class CollectionParameterExtractorSpec extends Specification with Mockito {

  "Extractor with default" should {

    val extractor = new CollectionParameterExtractor[Set[String]]("name", "default")

    "have a name" in {
      extractor.getName must be("name")
    }

    "extract parameter values" in {
      val params = new MultivaluedMapImpl()
      params.add("name", "one")
      params.add("name", "two")
      params.add("name", "three")

      extractor.extract(params) must_== (Set("one", "two", "three"))
    }

    "uses default if no parameter exists" in {
      extractor.extract(new MultivaluedMapImpl()) must_== (Set("default"))
    }
  }

  "Extractor without default" should {

    val extractor = new CollectionParameterExtractor[Set[String]]("name", null)

    "return empty collection" in {
      extractor.extract(new MultivaluedMapImpl()) must_== (Set.empty)
    }
  }
}
