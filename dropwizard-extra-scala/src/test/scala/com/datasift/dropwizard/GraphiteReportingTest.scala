package com.datasift.dropwizard

import config.{GraphiteConfiguration, GraphiteReportingConfiguration}
import org.junit.Assert._
import org.mockito.Mockito._

import org.junit.Test
import com.yammer.dropwizard.ScalaService
import com.yammer.dropwizard.config.{Configuration, Environment}
import reflect.BeanProperty

/**TODO: Document */
class GraphiteReportingTest {

  object TestService
    extends ScalaService[TestConfiguration]("test") with GraphiteReporting {
    def initialize(configuration: TestConfiguration, environment: Environment) {
    }
  }

  class TestConfiguration extends Configuration with GraphiteReportingConfiguration{
    @BeanProperty
    val graphite = new GraphiteConfiguration
  }

  @Test
  def initGraphiteService() {
    val conf = mock(classOf[TestConfiguration])
    val env = mock(classOf[Environment])

    TestService.initialize(conf, env)
    assertTrue(true)
  }
}
