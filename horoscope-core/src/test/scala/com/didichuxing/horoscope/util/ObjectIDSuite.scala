package com.didichuxing.horoscope.util

import org.scalatest.FunSuite

class ObjectIDSuite extends FunSuite with Logging {

  test("object id") {
    for(i <- 0 to 10)
      debug(ObjectID.next)
  }
}
