package com.didichuxing.horoscope.service.compositor

case class CompositorException(
  message: String, cause: Option[Throwable] = None
) extends Exception(message, cause.orNull)
