/*
 * Copyright (C) 2018 The DiDi, Inc. All Rights Reserved.
 * Authors: wenxiang@didichuxing.com
 */
package com.didichuxing.horoscope.util

import org.slf4j.{Logger, LoggerFactory}

trait Logging {
  @transient lazy val logging: Logger = LoggerFactory.getLogger(this.getClass)
  lazy val localIp = Utils.guessLocalIp()

  def debug(tag: String, traceInfo: String, message: (String, Any)*): Unit = {
    if (logging.isDebugEnabled) {
      logging.debug(logInfo(tag, traceInfo, message))
    }
  }

  def error(tag: String, traceInfo: String, message: (String, Any)*): Unit = {
    if (logging.isErrorEnabled) {
      logging.error(logInfo(tag, traceInfo, message))
    }
  }

  def info(tag: String, traceInfo: String, message: (String, Any)*): Unit = {
    if (logging.isInfoEnabled()) {
      logging.info(logInfo(tag, traceInfo, message))
    }
  }

  def warn(tag: String, traceInfo: String, message: (String, Any)*): Unit = {
    if (logging.isWarnEnabled()) {
      logging.warn(logInfo(tag, traceInfo, message))
    }
  }

  def debug(message: (String, Any)*): Unit = {
    if (logging.isDebugEnabled) {
      logging.debug(logInfo("horoscope", SystemLog.get(), message))
    }
  }

  def error(message: (String, Any)*): Unit = {
    if (logging.isErrorEnabled) {
      logging.error(logInfo("horoscope", SystemLog.get(), message))
    }
  }

  def info(message: (String, Any)*): Unit = {
    if (logging.isInfoEnabled()) {
      logging.info(logInfo("horoscope", SystemLog.get(), message))
    }
  }

  def warn(message: (String, Any)*): Unit = {
    if (logging.isWarnEnabled()) {
      logging.warn(logInfo("horoscope", SystemLog.get(), message))
    }
  }

  def logError(message: (String, Any)*): Unit = error(message: _*)

  def logInfo(message: (String, Any)*): Unit = info(message: _*)

  def logWarn(message: (String, Any)*): Unit = warn(message: _*)

  def logDebug(message: (String, Any)*): Unit = debug(message: _*)

  def debug(message: String): Unit = debug(("msg", message))

  def info(message: String): Unit = info(("msg", message))

  def warn(message: String): Unit = warn(("msg", message))

  def error(message: String): Unit = error(("msg", message))

  def logInfo(tag: String, traceInfo: String, message: Seq[(String, Any)]): String = {
    val sb = new StringBuilder
    sb.append(tag)
    sb.append("||").append("traceid=").append(traceInfo)
    sb.append("||").append("ip=").append(localIp)
    for (t <- message) {
      sb.append("||").append(t._1).append("=").append(t._2)
    }
    sb.toString()
  }

}
