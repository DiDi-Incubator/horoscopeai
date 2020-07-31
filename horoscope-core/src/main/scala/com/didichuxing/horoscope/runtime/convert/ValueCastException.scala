/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime.convert

case class ValueCastException(
  message: String, cause: Exception
) extends IllegalArgumentException(message, cause)
