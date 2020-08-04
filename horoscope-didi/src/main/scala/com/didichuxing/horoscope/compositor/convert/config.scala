/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.compositor.convert

import com.didichuxing.horoscope.runtime.{SimpleDict, Value}
import com.didichuxing.horoscope.apollo.convert.AnyRefConvertible
import com.typesafe.config.Config

import scala.collection.JavaConverters._

trait TypeSafeConfigConvertible {
  implicit val fromConfig: Value.From.Aux[Config, Value] = new TypeSafeConfigConverter
}

class TypeSafeConfigConverter extends Value.From[Config] with AnyRefConvertible {
  type ValueType = Value
  def apply(config: Config): Value = {
    if (config.isEmpty) {
      throw new Exception()
    } else {
      val confMap = Map.newBuilder[String, Value]
      config.root().unwrapped().asScala.foreach { case(key, v) =>
        confMap += key -> Value(v)
      }
      new SimpleDict(confMap.result())
    }
  }
}
