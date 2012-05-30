package com.datasift.dropwizard

import org.specs2.mutable.Specification
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import com.yammer.dropwizard.config.{Environment, Configuration}
import org.specs2.mock.Mockito


/**TODO: Document */
@RunWith(classOf[JUnitRunner])
class ComposableServiceSpec extends Specification with Mockito {

  "ComposableService" should {
    class TestConfiguration extends Configuration

    "register and run hooks in correct order initialization" in {
      object TestService extends ComposableService[TestConfiguration]("test") {
        var order = List.empty[Int]

        beforeInit ((conf: TestConfiguration, env: Environment) => {
          order = order :+ 1
        })

        afterInit ((conf: TestConfiguration, env: Environment) => {
          order = order :+ 3
        })

        def init(conf: TestConfiguration, env: Environment) { order = order :+ 2 }
      }

      TestService.initialize(mock[TestConfiguration], mock[Environment])
      TestService.order must_== List(1, 2, 3)
    }
  }
}
