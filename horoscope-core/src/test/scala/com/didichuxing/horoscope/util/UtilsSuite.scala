package com.didichuxing.horoscope.util

import com.didichuxing.horoscope.runtime.Value
import com.didichuxing.horoscope.util.Utils.{bucket, getSlot}
import org.scalatest.{FunSuite, Matchers, fixture}

class UtilsSuite extends FunSuite with Matchers {
  test("bucket") {
    val eventId = "60f7c632cf6f0537e7d34f0b"
    info(bucket(Value(eventId)).toString)
    info(bucket(Value(eventId), Some("suffix")).toString)
  }

  test("get context key") {
    val traceId = "010704d887473d19a71252770697c1bc"
    def getContextKey(traceId: String): String = {
      val slot = getSlot(traceId, 1024)
      val prefix = slot.formatted("%05d")
      if (false) {
        s"{$prefix}:$traceId"
      } else {
        s"$prefix:$traceId"
      }
    }
    val contextKey = getContextKey(traceId)
    info(contextKey)
  }

}
