/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.source

import java.util

import com.didichuxing.horoscope.mock.ClusterRunner
import com.didichuxing.horoscope.service.cluster.{FlowClient}
import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, FunSuite, Ignore}

@Ignore
class SourceSuite extends FunSuite with BeforeAndAfter with Logging {

  var client: FlowClient = _

  before {
    client = new ClusterRunner().run(ConfigFactory.load("application-remoting-2552.conf"))
  }

  after {
    client.stopAllSources()
  }

  test("register source") {
    client.removeSource("s1")
    val params = new util.HashMap[String, Any]()
    params.put("kafka.servers", "localhost:9092")
    params.put("kafka.group", "horoscope")
    params.put("kafka.max", "10")
    params.put("kafka.topic", "eventSource")
    params.put("grpc.port", "6880")
    params.put("backpress", "100")
  }

}
