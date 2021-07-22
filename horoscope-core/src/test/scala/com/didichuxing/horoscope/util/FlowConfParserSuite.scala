package com.didichuxing.horoscope.util

import com.didichuxing.horoscope.core.FlowConf.Bucket
import com.didichuxing.horoscope.util.FlowConfParser._
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConverters._

class FlowConfParserSuite extends FunSuite with Matchers {

  test("parse log conf") {
    val path = "flow-conf/topic.conf"
    val logConfigs = ConfigFactory.load(path).getConfigList("topics").asScala
    val topicLog = logConfigs.map(_.parseLogConf()).filter(_.topic == "topic1").head
    topicLog.flow shouldBe "/v2/topic/log-schedule"
    topicLog.fields.length shouldBe 8
    // info(topicLog.toString)

  }

  test("parse subscribe conf") {
    val path = "flow-conf/subscribe.conf"
    val subscribeConfigs = ConfigFactory.load(path).getConfigList("subscriptions").asScala
    val testSubscribe = subscribeConfigs.map(_.parseSubscribe).filter(_.name == "test-subscribe").head
    testSubscribe.subscriber shouldBe "flow2"
    testSubscribe.condition.get shouldBe ("city in [1, 2]")
    testSubscribe.args.size shouldBe 2
    testSubscribe.traffic shouldEqual (Bucket(0, 100))
    // info(testSubscribe.toString)
  }


  test("experiment conf") {
    val path = "flow-conf/experiment.conf"
    val experimentConf = ConfigFactory.load(path).getConfigList("experiments").asScala
    val testExperiment = experimentConf.map(_.parseABTestConf).filter(_.name == "parser-unit-test").head
    testExperiment.traffic shouldEqual Bucket(0, 10)
    testExperiment.condition.get shouldBe "city in [1]"
    testExperiment.target.get shouldBe "@event_id"
    testExperiment.groups.size shouldBe 2
    val treatment = testExperiment.groups.filter(_.group == "treatment").head
    treatment.flows.head._2.params shouldEqual Map("b" -> """{"key": 11}""")
    treatment.traffic shouldEqual Bucket(10, 100)
  }

  test("callback conf") {
    val path = "flow-conf/callback.conf"
    val callbackConf = ConfigFactory.load(path).getConfigList("callbacks").asScala
    val testCallback = callbackConf.map(_.parseCallbackConf()).filter(_.name == "test-callback").head
    testCallback.enabled shouldBe true
    testCallback.registerFlow shouldBe "/v2/callback/register"
    testCallback.timeout shouldBe "1 minute"
    testCallback.args.size shouldBe 2
  }
}
