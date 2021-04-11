package com.didichuxing.horoscope.util

import com.didichuxing.horoscope.core.Flow.Bucket
import com.didichuxing.horoscope.runtime.Value
import com.didichuxing.horoscope.util.FlowConfParser._
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConverters._

class FlowConfParserSuite extends FunSuite with Matchers {
  test("log conf") {
    val path = "flow-conf/log.conf"
    val logConfigs = ConfigFactory.load(path).getConfigList("logs").asScala
    val testLog = logConfigs.map(_.parseLogConf()).filter(_.flow == "test-log").head
    testLog.flow shouldBe "test-log"
    testLog.assigns.length shouldBe 2
    testLog.choices should contain theSameElementsAs(Seq("flow1", "flow2"))
    info(testLog.toString)
  }

  test("subscribe conf") {
    val path = "flow-conf/subscribe.conf"
    val subscribeConfigs = ConfigFactory.load(path).getConfigList("subscriptions").asScala
    val testSubscribe = subscribeConfigs.map(_.parseSubscribe).filter(_.name == "test-subscribe").head
    testSubscribe.subscriber shouldBe "flow2"
    testSubscribe.condition.get shouldBe ("city in [1, 2]")
    testSubscribe.args.size shouldBe 2
    testSubscribe.traffic shouldEqual (Bucket(0, 100))
    info(testSubscribe.toString)
  }


  test("experiment conf") {
    val path = "flow-conf/experiment.conf"
    val experimentConf = ConfigFactory.load(path).getConfigList("experiments").asScala
    val testExperiment = experimentConf.map(_.parseABTestConf).filter(_.name == "test-experiment").head
    testExperiment.condition.get shouldBe "city in [1]"
    testExperiment.target.get shouldBe "@event_id"
    testExperiment.groups.size shouldBe 2
    val treatment = testExperiment.groups.filter(_.group == "treatment-group").head
    treatment.params shouldEqual Map("b" -> Value(Map("key" -> Value(11))))
    treatment.traffic shouldEqual Bucket(10, 100)

    info(testExperiment.toString)
  }
}
