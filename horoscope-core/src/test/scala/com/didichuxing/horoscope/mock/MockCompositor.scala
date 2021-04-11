/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.mock

import com.didichuxing.horoscope.core.{Compositor, CompositorFactory}
import com.didichuxing.horoscope.runtime.{Value, ValueDict}
import com.didichuxing.horoscope.util.Logging

import scala.concurrent.Future

class MockCompositorFactory extends CompositorFactory {
  override def name(): String = "default"

  override def create(code: String)(resource: String => Array[Byte]): Compositor = {
    new MockCompositor(code)
  }
}

class MockCompositor(content: String) extends Compositor with Logging {
  override def composite(args: ValueDict): Future[Value] = {
    case class Result(link: String, need_infer: Boolean)
    val v = Value(Result("123", true))
    debug(("msg", "default compositor"), ("input", args), ("output", v))
    Future.successful(v)
  }
}
