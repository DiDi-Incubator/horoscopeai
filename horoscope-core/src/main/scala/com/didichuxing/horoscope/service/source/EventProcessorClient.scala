/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import java.util.concurrent.{Executor, LinkedBlockingQueue, TimeUnit}

import com.didichuxing.horoscope.core.EventProcessorServiceGrpc.EventProcessorServiceFutureStub
import com.didichuxing.horoscope.core.{EventBusService, EventProcessorServiceGrpc}
import com.didichuxing.horoscope.service.resource.Participant
import com.didichuxing.horoscope.util.Logging
import com.didichuxing.horoscope.util.Utils.configIntOrDefault
import com.typesafe.config.Config
import io.grpc.ManagedChannel
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import org.apache.commons.pool2.impl._
import org.apache.commons.pool2.{PooledObject, PooledObjectFactory}
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TFramedTransport, TSocket}

import scala.util.Try

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

class ThriftEventProcessorClient(participant: Participant, params: Config) extends EventProcessorClient with Logging {
  private val host = participant.getHost()
  private val port = params.getInt("rpc.port")
  private val clientSize = Try(params.getInt("rpc.client-size")).getOrElse(1)
  val nioEnable = Try(params.getBoolean("rpc.nio")).getOrElse(false)
  //for standalone cluster test
  /*val participantId = participant.getParticipantId()
  participantId match {
    case "192.168.1.7:2552" => port = 6880
    case "192.168.1.7:2553" => port = 6881
    case "192.168.1.7:2554" => port = 6882
  }*/
  debug(("msg", "create thrift client"), ("host", host), ("port", port))
  //val clientPool = new FixedClientPool(host, port, clientSize)
  val poolConfig = new GenericObjectPoolConfig[EventBusService.Client]()
  poolConfig.setMaxIdle(clientSize)
  poolConfig.setMaxTotal(clientSize)
  poolConfig.setMinIdle(clientSize)
  val clientPool = new GenericObjectPool[EventBusService.Client](new ThriftClientPoolFactory(),
    poolConfig, new AbandonedConfig)

  override def stub: EventBusService.Client = {
    debug(("msg", "get thrift client"))
    clientPool.borrowObject()
  }

  override def stop(): Unit = {
    clientPool.close()
  }

  override def toString: String = {
    s"thrift-$host:$port"
  }

  def release(client: EventBusService.Client): Unit = {
    debug(("msg", "release thrift client"))
    clientPool.returnObject(client)
  }

  def reconnect(client: EventBusService.Client): Unit = {
    debug(("msg", "reconnect thrift client"))
    clientPool.invalidateObject(client)
  }

  class FixedClientPool(host: String, port: Int, count: Int) {

    val protocolFactory = new TBinaryProtocol.Factory(true, true)
    val queue = new LinkedBlockingQueue[EventBusService.Client](count)
    val pool = new Array[EventBusService.Client](count)
    for (i <- 0 until count) {
      val t = newClient()
      pool(i) = t
      queue.put(t)
    }

    private def newClient(): EventBusService.Client = {
      val transport = if (!nioEnable) new TSocket(host, port) else new TFramedTransport(new TSocket(host, port))
      val protocol = protocolFactory.getProtocol(transport)
      val client = new EventBusService.Client(protocol)
      try {
        transport.open()
      } catch {
        case ex: Exception =>
          warn(("msg", "open thrift transport error"), ("ex", ex.getCause))
      }
      client
    }

    def borrowObject(): EventBusService.Client = {
      val t = queue.take()
      val transport = t.getInputProtocol().getTransport()
      if (!transport.isOpen) {
        try {
          transport.open()
        } catch {
          case ex: Exception =>
            warn(("msg", "open thrift transport error"), ("ex", ex.getCause))
        }
      }
      t
    }

    def returnObject(t: EventBusService.Client): Unit = {
      queue.put(t)
    }

    def invalidateObject(t: EventBusService.Client): Unit = {
      val transport = t.getInputProtocol().getTransport()
      if (transport.isOpen) {
        transport.close()
      }
    }

    def close(): Unit = {
      for (i <- 0 until count) {
        val transport = pool(i).getInputProtocol().getTransport()
        if (transport.isOpen) {
          transport.close()
        }
      }
    }
  }


  class ThriftClientPoolFactory extends PooledObjectFactory[EventBusService.Client] with Logging {

    val protocolFactory = new TBinaryProtocol.Factory(true, true)

    override def makeObject(): PooledObject[EventBusService.Client] = {
      val transport = if (!nioEnable) new TSocket(host, port) else new TFramedTransport(new TSocket(host, port))
      val protocol = protocolFactory.getProtocol(transport)
      val client = new EventBusService.Client(protocol)
      try {
        transport.open()
      } catch {
        case ex: Exception =>
          warn(("msg", "open thrift transport error"), ("ex", ex.getCause))
      }
      new DefaultPooledObject[EventBusService.Client](client)
    }

    //清除
    override def destroyObject(pooledObject: PooledObject[EventBusService.Client]): Unit = {
      val transport = pooledObject.getObject.getInputProtocol().getTransport()
      if (transport.isOpen) {
        transport.close()
      }
    }

    //校验
    override def validateObject(pooledObject: PooledObject[EventBusService.Client]): Boolean = {
      val transport = pooledObject.getObject.getInputProtocol().getTransport()
      transport.isOpen
    }

    //激活
    override def activateObject(pooledObject: PooledObject[EventBusService.Client]): Unit = {
      val transport = pooledObject.getObject.getInputProtocol().getTransport()
      if (!transport.isOpen) {
        try {
          transport.open()
        } catch {
          case ex: Exception =>
            warn(("msg", "open thrift transport error"), ("ex", ex.getCause))
        }
      }
    }

    //钝化
    override def passivateObject(pooledObject: PooledObject[EventBusService.Client]): Unit = {

    }
  }

}
