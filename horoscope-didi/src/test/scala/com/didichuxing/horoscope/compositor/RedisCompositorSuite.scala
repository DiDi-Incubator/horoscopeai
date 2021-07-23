/*
 * Copyright (C) 2021 DiDi Inc. All Rights Reserved.
 * Authors: chenyiran@didiglobal.com
 * Description:
 */
package com.didichuxing.horoscope.compositor

import akka.actor.ActorSystem
import com.didichuxing.horoscope.runtime.{SimpleDict, Value}
import org.scalatest.FunSuite

import java.util.UUID
import scala.concurrent.ExecutionContextExecutor

class RedisCompositorSuite extends FunSuite {

  import com.didichuxing.horoscope.util.AsyncUtil._

  implicit val actorSystem: ActorSystem = ActorSystem("rest-compositor-suite")
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher
  private val factory = new RedisCompositorFactory
  // 10.179.222.85:9000为质量效能平台测试fusion
  // http://base.xiaojukeji.com/console/empower/?project_id=d_map-1205561#/fusion/2727
  private val getCode =
  """
method = "get"
host = "10.179.20.80"
port = 9000
"""
  private val setCode =
    """
method = "set"
host = "10.179.20.80"
port = 9000
"""

  ignore("testComposite") {
    val setCompositor = factory.create(setCode)(_ => Array.empty)
    val key = Value(UUID.randomUUID().toString)
    val value = new SimpleDict(Map("a" -> Value(1), "b" -> Value("2")))
    val req = new SimpleDict(Map("key" -> key, "value" -> value))
    setCompositor.composite(req).await()

    val getCompositor = factory.create(getCode)(_ => Array.empty)
    val respFuture = getCompositor.composite(req)
    assert(respFuture.await().equals(value))
  }

}
