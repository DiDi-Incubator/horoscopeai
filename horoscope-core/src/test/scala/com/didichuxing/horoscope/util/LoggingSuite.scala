/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.util

import org.scalatest.FunSuite

class LoggingSuite extends FunSuite with Logging {
  test("test") {
    logging.info("hello world!")
  }
}
