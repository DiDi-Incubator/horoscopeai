/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.source

import com.didichuxing.horoscope.mock.LocalRunner
import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, FunSuite, Ignore}

class LocalSuite extends FunSuite with BeforeAndAfter with Logging {
  before {
    new LocalRunner().run(ConfigFactory.load("application-local.conf"))
  }

  test("run server") {
    //启动后，主进程休眠3s
    Thread.sleep(3000)
  }

}
