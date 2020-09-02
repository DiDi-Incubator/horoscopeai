/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.resource

import com.didichuxing.horoscope.service.resource.ZkClient
import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, FunSuite, Ignore}
import org.apache.curator.test.TestingServer

@Ignore
class ZkSuite extends FunSuite with BeforeAndAfter with Logging {

  test("zk create") {
    val server = new TestingServer(2181, true)
    val config = ConfigFactory.load("application-remoting-2552.conf")
    val zkClient = new ZkClient(config)
    debug(("msg", zkClient.create("/demo/my")))
    zkClient.stop()
    server.close()
  }

  test("child") {
    val server = new TestingServer(2181, true)
    val config = ConfigFactory.load("application-remoting-2552.conf")
    val zkClient = new ZkClient(config)
    zkClient.create("/demo/my")
    debug(("msg", zkClient.getChild("/demo")))
    zkClient.stop()
    server.close()
  }

}
