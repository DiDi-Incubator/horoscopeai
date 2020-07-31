/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.console.service

import com.didichuxing.horoscope.runtime.{Value, ValueDict}
import com.didichuxing.horoscope.runtime.expression.Expression
import com.didichuxing.horoscope.util.Logging
import org.springframework.http.{HttpStatus, MediaType}
import org.springframework.web.bind.annotation._

@RestController
@RequestMapping(Array("/api/toolbox"))
class Toolbox extends Logging {
  import com.didichuxing.horoscope.runtime.Implicits._

  @RequestMapping(
    value = Array("/evaluate"),
    method = Array(RequestMethod.GET, RequestMethod.POST),
    produces = Array(MediaType.APPLICATION_JSON_UTF8_VALUE))
  @ResponseStatus(HttpStatus.OK)
  def evaluate(@RequestParam expression: String, @RequestParam context: String): String = {
    logInfo("expression" -> expression)
    Expression(expression).apply(gson.fromJson(context, classOf[Value]).as[ValueDict]).toJson
  }
}
