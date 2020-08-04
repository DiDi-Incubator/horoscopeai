/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.compositor

import java.net.URLEncoder
import com.didichuxing.horoscope.runtime.{NULL, Text, ValueDict}
import com.didichuxing.horoscope.compositor.RestfulCompositorConfig.RestfulConfigInFlow
import scala.util.Try
import scala.util.matching.Regex

object CompositorUtil {
  def parseConfigFromFlow(code: String): RestfulConfigInFlow = {
    val lines = code.split("\n")
    val fields = lines(0).split(" ")
    val method = fields.head
    val url = Try(fields(1)).getOrElse("")
    val serviceName = Try(Some(fields(2))).getOrElse(None)
    val postBodyKey = Try(Some(lines(1).split(" ").head)).getOrElse(None)
    RestfulConfigInFlow(method, url, serviceName, postBodyKey)
  }

  def updateUrlByConfig(url: String, args: ValueDict): String = {
    replaceAllPatterns(url, args, CONFIG_PATTERN, false)
  }

  def updateUrlByArguments(url: String, args: ValueDict): String = {
    replaceAllPatterns(url, args, ARGUMENT_PATTERN, true)
  }

  def replaceAllPatterns(url: String, args: ValueDict, pattern: Regex, encode: Boolean): String = {
    val variableNames = for (m <- pattern.findAllMatchIn(url)) yield (m.group(0), m.group(1))
    var resultUrl = url
    for ((group0, variable) <- variableNames) {
      val value = args.visit(variable)
      val valueStr = if (value == NULL) {
        throw CompositorException(s"Compositor reference: ${variable} is not set")
      } else if (value.isInstanceOf[Text]) {
        value.as[Text].underlying
      } else {
        value.toString
      }
      val encodeValue = if (encode) {
        URLEncoder.encode(valueStr, "UTF-8")
      } else {
        valueStr
      }
      resultUrl = resultUrl.replace(group0, encodeValue)
    }
    resultUrl
  }

  val ARGUMENT_PATTERN = "\\$\\{([^{}]+?)\\}".r
  val CONFIG_PATTERN = "@\\{([^{}]+?)\\}".r
}
