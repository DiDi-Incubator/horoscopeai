/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime.ops

import com.didichuxing.horoscope.runtime.{NumberValue, Text}


trait OrderingImplicits {
  implicit object TextOrdering extends TextOrdering
}

trait TextOrdering extends Ordering[Text] {
  def compare(x: Text, y: Text): Int = x.underlying.compare(y.underlying)
}

trait NumberOrdering extends Ordering[NumberValue] {
  def compare(x: NumberValue, y: NumberValue): Int = x.underlying.compare(y.underlying)
}

