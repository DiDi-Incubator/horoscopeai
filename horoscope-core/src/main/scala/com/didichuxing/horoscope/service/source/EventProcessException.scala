/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import com.didichuxing.horoscope.service.source.EventProcessErrorCode.ErrorCode

object EventProcessErrorCode extends Enumeration {
  type ErrorCode = Value
  val Success = Value(0, "success")
  //执行期间错误
  val TimeOutError = Value(100, "execute_timeout")
  val BackPressError = Value(101, "backpress_timeout")
  val CheckError = Value(102, "commit_check_error") //提交检查，可能不属于本分区
  val CommitError = Value(103, "commit_error") //提交异常
  val ExecuteError = Value(104, "execute_error") //执行错误
  //请求前错误
  val FormatError = Value(105, "event_format_error") //消息格式错误
  val RpcClientError = Value(106, "rpc_client_not_found") //无法获取rpc client

  val SourceError = Value(107, "source_error")//Source类型错误

  val UnknownError = Value(999, "unknown_error")
}

case class EventProcessException(code: ErrorCode) extends Exception
