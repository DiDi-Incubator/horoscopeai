/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.compositor

case class CompositorException(
  message: String, cause: Option[Throwable] = None
) extends Exception(message, cause.orNull)
