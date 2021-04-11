package com.didichuxing.horoscope.experiment

import com.didichuxing.horoscope.runtime.experiment.ABTestControllerFactory
import com.didichuxing.horoscope.runtime.expression.{BuiltIn, DefaultBuiltIn}
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}
import com.didichuxing.horoscope.runtime._
import com.didichuxing.horoscope.util.Utils

class ABTestControllerSuite extends FunSuite with Matchers {
  val builtIn: BuiltIn = DefaultBuiltIn.defaultBuiltin

  val exprConfig = ConfigFactory.parseString(
    """
      |{
      |        name = "test-experiment"
      |        catalog = "ab_test"
      |        enabled = true
      |        flow = "flow"
      |        condition = "city in [1]"
      |        target = "link_id"
      |        groups = [
      |            {
      |                name = "control-group"
      |                flow = "flow1"
      |                args = []
      |                bucket = [0, 10]
      |            },
      |            {
      |                name = "treatment-group"
      |                flow = "flow2"
      |                args = []
      |                bucket = [10, 100]
      |            }
      |        ]
      |    }
      |""".stripMargin)
  test("ab-test") {
    val controller = new ABTestControllerFactory().create(exprConfig)(builtIn)
    controller.dependency.keys.toSeq should contain theSameElementsAs List("city", "link_id")

    // condition satisfied
    val linkId = 192092019291L
    val bucket = Utils.bucket(Value(192092019291L))
    val context1 = Map("city" -> Value(1), "link_id" -> Value(linkId))
    val choice1 = controller.evaluate(Value(context1))
    if (bucket < 10) {
      choice1.flow shouldBe "flow1"
      choice1.group shouldBe "control-group"
    } else {
      choice1.flow shouldBe "flow2"
      choice1.group shouldBe "treatment-group"
    }

    // condition not satisfied
    val context2 = Map("city" -> Value(2), "link_id" -> Value(linkId))
    val choice2 = controller.evaluate(Value(context2))
    choice2.flow shouldBe "flow"
    choice2.group shouldBe "default_group"
  }

}
