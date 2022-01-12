package com.didichuxing.horoscope.util

import com.didichuxing.horoscope.runtime.Value
import com.didichuxing.horoscope.service.resource.SlotRange
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

  test("slot") {
    val parts = Range(0, 9).map(_.toString + "-part").toArray
    val slotCount = 100
    val size = parts.size
    //每个参与者负责的slot数量
    val count = slotCount / size
    //余数
    val rest = slotCount % size
    for (i <- parts) {
      val partWithIdx = parts.zipWithIndex.filter(p => p._1 == i)
      if (partWithIdx.size > 0) {
        val idx = partWithIdx(0)._2
        var begin = 0
        var end = 0
        //最后一个将余数加上
        if (rest > 0 && idx < rest) {
          begin += idx * (count + 1)
          end += begin + count + 1
        } else {
          begin += idx * count + rest
          end = begin + count
        }
        info(s"index: ${i}, begin: ${begin}, end: ${`end`}")
        Some(SlotRange(begin, end))
      } else {
        None
      }
    }
  }

}
