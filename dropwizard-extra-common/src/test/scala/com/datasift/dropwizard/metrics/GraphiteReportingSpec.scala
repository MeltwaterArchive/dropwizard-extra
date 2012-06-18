package com.datasift.dropwizard.metrics

import config.{GraphiteReportingConfiguration, GraphiteConfiguration}
import health.GraphiteHealthCheck
import org.specs2.mutable.Specification
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.mock.Mockito
import com.yammer.dropwizard.config.{Environment, Configuration}
import com.yammer.dropwizard.{ScalaService, Logging}

/** Specification for GraphiteReporting utility */
@RunWith(classOf[JUnitRunner])
class GraphiteReportingSpec extends Specification with Mockito {

  "GraphiteReportingBundle" should {
    class TestServiceConfiguration extends Configuration with GraphiteReportingConfiguration
    object TestService
      extends TemporaryScalaService[TestServiceConfiguration]("test")
      with Logging
      with GraphiteReporting
    {
      def initialize(conf: TestServiceConfiguration, env: Environment) {
        println("hello?")
      }
    }

    "add no healthcheck when disabled" in {
      val env = mock[Environment]
      TestService.initialize(new TestServiceConfiguration, env)
      there was no(env).addHealthCheck(any[GraphiteHealthCheck])
    }

    "add a healthcheck when enabled" in {
      val env = mock[Environment]
      val conf = new TestServiceConfiguration {
        override val graphite = new GraphiteConfiguration {
          override val enabled = true
          override val host = "test"
          override val port = 1234
          override val prefix = "prefix"
        }
      }
      TestService.initializeWithBundles(conf, env)
      there was one(env).addHealthCheck(any[GraphiteHealthCheck])
    }
  }
}
