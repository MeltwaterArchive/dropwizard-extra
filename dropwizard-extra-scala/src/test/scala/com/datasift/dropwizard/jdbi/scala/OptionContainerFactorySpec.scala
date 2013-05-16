package com.datasift.dropwizard.jdbi.scala

import org.specs2.mutable._
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import scala.collection.immutable.{SortedSet, HashSet}

/**
 * Tests [[com.datasift.dropwizard.jdbi.scala.OptionContainerFactory]]
 */
@RunWith(classOf[JUnitRunner])
class OptionContainerFactorySpec extends Specification {

  "OptionContainerFactory for Ints" should {
    val factory = new OptionContainerFactory

    "Accepts Options" in {
      factory.accepts(classOf[Option[Int]]) must beTrue
    }

    "Doesn't accept Lists" in {
      factory.accepts(classOf[List[Int]]) must beFalse
    }

    "Builds a None by default" in {
      factory.newContainerBuilderFor(classOf[Int])
        .build() must beNone
    }

    "Builds a Some of an Int on demand" in {
      factory.newContainerBuilderFor(classOf[Int]).add(123)
        .build() must beSome(123)
    }

    "Builds a Some of the last Int on demand" in {
      factory.newContainerBuilderFor(classOf[Int]).add(123).add(456)
        .build() must beSome(456)
    }
  }

  "OptionContainerFactory for Strings" should {
    val factory = new OptionContainerFactory

    "Accepts Options" in {
      factory.accepts(classOf[Option[String]]) must beTrue
    }

    "Doesn't accept Lists" in {
      factory.accepts(classOf[List[String]]) must beFalse
    }

    "Builds a None by default" in {
      factory.newContainerBuilderFor(classOf[String])
        .build() must beNone
    }

    "Builds a Some of a String on demand" in {
      factory.newContainerBuilderFor(classOf[String]).add("abc")
        .build() must beSome("abc")
    }

    "Builds a Some of the last String on demand" in {
      factory.newContainerBuilderFor(classOf[String]).add("abc").add("def")
        .build() must beSome("def")
    }
  }
}
