package com.datasift.dropwizard.metrics

import org.specs2.mutable.Specification
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.mock.Mockito
import com.datasift.dropwizard.ComposableService
import com.yammer.dropwizard.Logging
import com.yammer.dropwizard.config.{Environment, Configuration}
import com.datasift.dropwizard.health.GraphiteHealthCheck
import com.datasift.dropwizard.conf.{GraphiteConfiguration, ConfigurableGraphiteReporting}

/** Specification for GraphiteReporting utility */
@RunWith(classOf[JUnitRunner])
class GraphiteReportingSpec extends Specification with Mockito {

  "GraphiteReporting Service helper" should {
    class TestServiceConfiguration extends Configuration with ConfigurableGraphiteReporting
    object TestService
      extends ComposableService[TestServiceConfiguration]("test")
      with Logging
      with GraphiteReporting
    {
      def init(conf: TestServiceConfiguration, env: Environment) {
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
      TestService.initialize(conf, env)
      there was one(env).addHealthCheck(any[GraphiteHealthCheck])
    }
  }
}
