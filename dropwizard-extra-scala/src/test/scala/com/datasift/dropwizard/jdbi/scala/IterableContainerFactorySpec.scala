package com.datasift.dropwizard.jdbi.scala

import org.specs2.mutable._
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import scala.collection.immutable.{SortedSet, HashSet}

/**
 * Tests [[com.datasift.dropwizard.jdbi.scala.IterableContainerFactory]]
 */
@RunWith(classOf[JUnitRunner])
class IterableContainerFactorySpec extends Specification {

  "IterableContainerFactory for Seqs" should {
    val factory = new IterableContainerFactory[Seq]

    "Accepts Seqs" in {
      factory.accepts(classOf[Seq[Int]]) must beTrue
    }

    "Accepts Lists" in {
      factory.accepts(classOf[List[Int]]) must beTrue
    }

    "Accepts Vectors" in {
      factory.accepts(classOf[Vector[Int]]) must beTrue
    }

    "Builds an empty Seq" in {
      factory.newContainerBuilderFor(classOf[Int])
        .build() must beEmpty[Seq[_]]
    }

    "Builds a Seq of Ints on demand" in {
      factory.newContainerBuilderFor(classOf[Int]).add(123)
        .build() must beEqualTo(Seq(123))
    }

    "Builds a Seq of Strings on demand" in {
      factory.newContainerBuilderFor(classOf[String]).add("abc").add("def")
        .build() must beEqualTo(Seq("abc", "def"))
    }
  }

  "IterableContainerFactory for Sets" should {
    val factory = new IterableContainerFactory[Set]

    "Accepts Sets" in {
      factory.accepts(classOf[Set[Int]]) must beTrue
    }

    "Accepts Lists" in {
      factory.accepts(classOf[SortedSet[Int]]) must beTrue
    }

    "Accepts Vectors" in {
      factory.accepts(classOf[HashSet[Int]]) must beTrue
    }

    "Builds an empty Set" in {
      factory.newContainerBuilderFor(classOf[Int])
        .build() must beEmpty[Set[_]]
    }

    "Builds a Set of Ints on demand" in {
      factory.newContainerBuilderFor(classOf[Int]).add(123)
        .build() must beEqualTo(Set(123))
    }

    "Builds a Set of Strings on demand" in {
      factory.newContainerBuilderFor(classOf[String]).add("abc").add("def")
        .build() must beEqualTo(Set("abc", "def"))
    }
  }
}
