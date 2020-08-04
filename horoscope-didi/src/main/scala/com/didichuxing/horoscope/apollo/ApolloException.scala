/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */
package com.didichuxing.horoscope.apollo

case class ApolloException(
  message: String, cause: Option[Throwable] = None
) extends Exception(message, cause.orNull)
