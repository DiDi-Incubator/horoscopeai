/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 */

package com.didichuxing.horoscope.runtime

/**
  * exception that event process will not retry just discard
  */
case class IgnoredException(message: String) extends Exception(message)
