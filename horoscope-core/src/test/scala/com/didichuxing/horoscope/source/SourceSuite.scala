/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.source

import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import org.scalatest.{BeforeAndAfter, FunSuite, Ignore}

class SourceSuite extends FunSuite with BeforeAndAfter with Logging {

  test("source config") {
    val content = """{
                    |      factory-name = "batchJsonKafka"
                    |      source-name = "s1"
                    |      flow-name = "/root/v2/flow1"
                    |      parameter {
                    |        kafka = {
                    |          servers = "10.179.24.223:9093"
                    |          cluster-id = 95
                    |          app-id = "appId_001485"
                    |          password = "_twYiKJNrBCX"
                    |          topic = "event_source"
                    |          group = "test0713"
                    |          max = 6
                    |          concurrency = 1
                    |        }
                    |        rpc {
                    |          type = "thrift"
                    |          port = 6880
                    |        }
                    |        backpress {
                    |          permits = 10
                    |          timeout = 60
                    |        }
                    |      }
                    |    }""".stripMargin
    val config = ConfigFactory.parseString(content)
    println(config.root().render(ConfigRenderOptions.concise()))
  }

}
