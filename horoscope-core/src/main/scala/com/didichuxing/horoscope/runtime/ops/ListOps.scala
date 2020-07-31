/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime.ops

import com.didichuxing.horoscope.runtime._

class ListOps(val repr: ValueList) extends AnyVal {
  def elementType: Option[String] = {
    repr.children match {
      case head +: tail =>
        val headType = head.valueType
        if (tail.forall(_.valueType == headType)) {
          Some(headType)
        } else {
          None
        }
      case _ =>
        None
    }
  }
}
