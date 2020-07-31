/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.resource

import com.didichuxing.horoscope.util.{Logging, Utils}
import org.scalatest.{BeforeAndAfter, FunSuite}

class NetSuite extends FunSuite with BeforeAndAfter with Logging {

  test("local ip") {
    info(("ip", Utils.guessLocalIp()))
  }

}
