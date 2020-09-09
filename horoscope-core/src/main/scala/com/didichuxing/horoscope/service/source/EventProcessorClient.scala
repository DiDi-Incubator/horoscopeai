/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import java.util.concurrent.{Executor, TimeUnit}

import com.didichuxing.horoscope.core.EventProcessorServiceGrpc.EventProcessorServiceFutureStub
import com.didichuxing.horoscope.core.EventProcessorServiceGrpc
import com.didichuxing.horoscope.service.resource.Participant
import com.didichuxing.horoscope.util.Logging
import com.didichuxing.horoscope.util.Utils.configIntOrDefault
import com.typesafe.config.Config
import io.grpc.ManagedChannel
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder

trait EventProcessorClient {

  def stub: AnyRef

  def stop()

}

class GRPCEventProcessorClient(participant: Participant, params: Config)
                              (implicit ec: Executor) extends EventProcessorClient with Logging {

  private val host = participant.getHost()
  private val port = params.getInt("rpc.port")
  //for standalone cluster test
  /*val participantId = participant.getParticipantId()
  participantId match {
    case "127.0.0.1:2552" => port = 6880
    case "127.0.0.1:2553" => port = 6881
    case "127.0.0.1:2554" => port = 6882
  }*/
  debug(("msg", "create grpc client"), ("host", host), ("port", port))
  private val window = configIntOrDefault(params, "rpc.window", 102400) * 1024
  private val channel: ManagedChannel = NettyChannelBuilder.forAddress(host, port).executor(ec)
    .flowControlWindow(window).usePlaintext.build
  private val _stub = EventProcessorServiceGrpc.newFutureStub(channel)

  override def stub: EventProcessorServiceFutureStub = _stub

  override def stop(): Unit = {
    channel.shutdown().awaitTermination(10, TimeUnit.SECONDS)
  }

  override def toString: String = {
    s"gprc-$host:$port"
  }
}
