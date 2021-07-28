package com.didichuxing.horoscope.experiment

import com.didichuxing.horoscope.runtime.experiment.ABTestControllerFactory
import com.didichuxing.horoscope.runtime.expression.{BuiltIn, DefaultBuiltIn}
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}
import com.didichuxing.horoscope.runtime._
import com.didichuxing.horoscope.util.Utils

import scala.collection.mutable.ArrayBuffer

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
      |        traffic = [0, 100]
      |        groups = [
      |            {
      |                name = "control-group"
      |                flows = []
      |                bucket = [0, 10]
      |            },
      |            {
      |                name = "treatment-group"
      |                flows = []
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
    val bucket = Utils.bucket(Value(192092019291L), Some("test-experiment"))
    val context1 = Map("city" -> Value(1), "link_id" -> Value(linkId))
    val plan = controller.query(Value(context1)).head
    if (bucket < 10) {
      plan.group shouldBe "control-group"
    } else {
      plan.group shouldBe "treatment-group"
    }

    // condition not satisfied
    val context2 = Map("city" -> Value(2), "link_id" -> Value(linkId))
    val choice2 = controller.query(Value(context2))
    choice2 shouldBe None
  }

  test("bucket randomness verification") {
    var count = 1
    val arrayBuffer = new ArrayBuffer[Int]()
    for (i <- Range(0, 10)) {
      arrayBuffer.append(0)
    }

    while(count < 100) {
      val eventId = Utils.getEventId()
      val bucketId = Utils.getSlot(eventId, 100)
      if (bucketId >= 0 && bucketId < 1) {
        count = count + 1
        val tag = "123"
        val tagBucketId = Utils.bucket(Value(eventId), Some(tag))
        val index = tagBucketId / 10
        arrayBuffer.update(index, arrayBuffer(index) + 1)
      }
    }
  }

}
