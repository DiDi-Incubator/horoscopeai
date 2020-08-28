/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: liangguorong@didiglobal.com
 */

package com.didichuxing.horoscope.ods

import com.didichuxing.horoscope.ods.util.DateUtil
import org.scalatest.{FunSuite, Matchers}

class DateUtilSuite extends FunSuite with Matchers {

  test("date ranges") {
    val v = DateUtil.dateRange("2020-07-23 15:00", -1)
    info(v.mkString("[", ",", "]"))
  }

}
