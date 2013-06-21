package com.datasift.dropwizard.jdbi.scala

import org.skife.jdbi.v2.ContainerBuilder
import org.skife.jdbi.v2.tweak.ContainerFactory

/** A [[org.skife.jdbi.v2.tweak.ContainerFactory]] for Scala Options. */
class OptionContainerFactory extends ContainerFactory[Option[Any]] {

  def accepts(clazz: Class[_]): Boolean = classOf[Option[_]].isAssignableFrom(clazz)

  def newContainerBuilderFor(clazz: Class[_]): ContainerBuilder[Option[Any]] = {
    new ContainerBuilder[Option[Any]] {

      var option: Option[Any] = None

      def add(it: Any): ContainerBuilder[Option[Any]] = {
        option = Option(it)
        this
      }

      def build(): Option[Any] = option
    }
  }
}


