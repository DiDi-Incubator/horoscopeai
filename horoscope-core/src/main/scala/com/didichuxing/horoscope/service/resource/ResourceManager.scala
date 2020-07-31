/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.resource

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent._
import java.util.concurrent.locks.ReentrantLock

import akka.actor.{Actor, ActorSelection, Cancellable, Props}
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.service.source.{EventBusFactory, EventProcessorClient}
import com.didichuxing.horoscope.util.Constants._
import com.didichuxing.horoscope.util.Utils._
import com.didichuxing.horoscope.util.{Logging, SystemLog}
import com.typesafe.config.Config
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type
import org.apache.curator.framework.recipes.cache._
import org.apache.curator.framework.recipes.leader.{LeaderLatch, LeaderLatchListener}

import scala.collection.JavaConversions._
import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
 * range左闭合，右开，[begin, end)
 *
 * @param begin
 * @param end
 */
case class SlotRange(begin: Int, end: Int)

/**
 * 维护cluster的扩缩容，将数据按2^14进行划片，每个服务器负责一定数量的分片
 * 1.每台机器负责的traceId发送变化
 *    1.1 本来属于本机执行的，不再需要本机执行。
 *    1.2 本来不属于本机执行的，现在需要本机执行。
 * 2.每台机器负责的数据分片发送变化
 * 3.由于scheduler的数据存在本地，需要重新加载
 * 4.只有ping触发机器摘除（重新分区），zk负责机器扩容（如果已经在集群中restart，否则重新分区）
 **/
trait ResourceManager {
  /**
   * 启动
   */
  def start()

  /**
   * 关闭
   */
  def stop()

  /**
   * 获取集群参与者信息
   *
   * @return
   */
  def getParticipants(): List[Participant]

  /**
   * 获取本机负责的slot范围
   *
   * @return
   */
  def getSlotRange(participant: Participant): Option[SlotRange]

  /**
   * 获取本机的信息
   *
   * @return
   */
  def local(): Participant

  /**
   * 获取source rpc client
   *
   * @param participantId
   * @param sourceName
   */
  def getRPCClient(participantId: String, sourceName: String, params: Config): Option[EventProcessorClient]

  /**
   * 注册rpc client
   *
   * @param sourceName
   */
  def registerRPCClient(sourceName: String, params: Config)

  /**
   * 取消数据源注册
   *
   * @param sourceName
   */
  def removeRPCClient(sourceName: String, params: Config)

}

//选主完成
case class LeaderSelectorMessage(logId: String)

//注册完成
case class RegisterCompleteMessage(logId: String, master: Participant, participants: List[Participant])

//扩缩容
case class SlaverRestartMessage(logId: String, master: Participant, participants: List[Participant])

case class PingMessage(logId: String)

case class PongMessage(logId: String, success: Boolean)

//某个source所有参与者的rpc clients
case class RPCClients(params: Config, clients: ConcurrentHashMap[String, EventProcessorClient])

/**
 * 基于zookeeper的选主服务
 */
class DefaultResourceManager(implicit ctx: ApplicationContext) extends ResourceManager with Logging {

  implicit val config = ctx.config
  implicit val system = ctx.system
  implicit val zkClient = ctx.zkClient
  implicit val scheduler = ctx.scheduler
  implicit val ec = ctx.sourceExecutionContext.getExecutionContext()
  val masterSelectorPath = config.getString("horoscope.zookeeper.cluster.path")
  var localParticipantId: String = ""
  //启动时，第一次选主的回调
  val leaderSelectorPromise = Promise[Participant]()
  //master监听slaver注册
  var leaderLatch: LeaderLatch = _
  var zkWatcher: PathChildrenCache = _
  var pingSchedule: Cancellable = _
  //是否需要再分区
  private val isRepartition = new AtomicBoolean(false)
  //所有当前参与者，participants，sourceRPC，leaderSelectorActors需要扩缩容同步，ConcurrentSkipListMap保证按key排序
  var participants = new ConcurrentSkipListMap[String, Participant]()
  //结构：sourceName@port:participantId:client
  var sourceRPCClients = new ConcurrentHashMap[String, RPCClients]()
  //分区发生变化后的列表
  var reParticipants = new ConcurrentHashMap[String, Participant]()
  //participant变更公平锁
  val participantLock = new ReentrantLock(true)
  //slot数量
  val slotCount = getClusterSlotCount(config)

  override def registerRPCClient(sourceName: String, params: Config): Unit = {
    val port = params.getInt("rpc.port")
    val sourceKey = s"$sourceName$SOURCEKEY_MARK$port"
    sourceKey.intern().synchronized {
      val sourceClients = sourceRPCClients.getOrDefault(sourceKey,
        RPCClients(params, new ConcurrentHashMap[String, EventProcessorClient]))
      for (participant <- participants.values()) {
        val participantId = participant.getParticipantId()
        val clients = sourceClients.clients
        if (clients.get(participantId) == null) {
          debug(("msg", "create rpc client"), ("local", localParticipantId), ("participantId", participantId))
          clients.put(participantId, EventBusFactory.newClient(participant, params))
        } else {
          debug(("msg", "has register rpc client"), ("source", sourceKey), ("participantId", participantId))
        }
      }
      sourceRPCClients.put(sourceKey, sourceClients)
    }
  }

  override def removeRPCClient(sourceName: String, params: Config): Unit = {
    val port = params.getInt("rpc.port")
    val sourceKey = s"$sourceName$SOURCEKEY_MARK$port"
    sourceKey.intern().synchronized {
      val sourceClients = sourceRPCClients.get(sourceKey)
      if (sourceClients != null) {
        val clients = sourceClients.clients
        for (client <- clients.values()) {
          client.stop()
        }
      }
      sourceRPCClients.remove(sourceKey)
    }
  }

  override def getRPCClient(participantId: String, sourceName: String, params: Config): Option[EventProcessorClient] = {
    val port = params.getInt("rpc.port")
    val sourceKey = s"$sourceName$SOURCEKEY_MARK$port"
    val sourceClients = sourceRPCClients.get(sourceKey)
    if (sourceClients != null) {
      val client = sourceClients.clients.get(participantId)
      Option(client)
    } else {
      None
    }
  }

  private def getLeaderSelectorActor(participantId: String): ActorSelection = {
    val path = s"akka.tcp://${system.name}@${participantId}/user/LeaderSelectorActor"
    system.actorSelection(path)
  }

  /**
   * 通过traceId获取集群参与者信息
   *
   * @return
   */
  override def getParticipants(): List[Participant] = {
    participants.values().toList
  }


  /**
   * 获取本机分配的solt
   *
   * @return
   */
  override def getSlotRange(participant: Participant): Option[SlotRange] = {
    val parts = getParticipants()
    //参与者总数
    val size = parts.size
    //每个参与者负责的slot数量
    val count =  slotCount / size
    //余数
    val rest = slotCount % size
    val partWithIdx = parts.zipWithIndex.filter(p => p._1.getParticipantId() == participant.getParticipantId())
    if (partWithIdx.size > 0) {
      val idx = partWithIdx(0)._2
      val begin = idx * count
      var end = begin + count
      //最后一个将余数加上
      if (rest > 0 && idx == (size - 1)) {
        end += rest
      }
      Some(SlotRange(begin, end))
    } else {
      None
    }
  }

  /**
   * 获取本机的信息
   *
   * @return
   */
  override def local(): Participant = {
    participants.get(localParticipantId)
  }

  override def start(): Unit = {
    info("begin start resourceManger")
    //启动actor，开始接收消息
    system.actorOf(Props(new LeaderSelectorActor), "LeaderSelectorActor")
    //config 配置的参与者
    for (parId <- config.getStringList("horoscope.cluster")) {
      val hostPort = parId.split(":")
      if (hostPort.size == 2) {
        val par = new Participant(hostPort(0), hostPort(1).toInt)
        participants.put(parId, par)
      }
    }
    //获取本机IP
    val host = Try(config.getString("akka.remote.netty.tcp.hostname")).getOrElse(guessLocalIp())
    val port = config.getInt("akka.remote.netty.tcp.port")
    localParticipantId = s"${host}:${port}"
    var localPart = participants.get(localParticipantId)
    if (localPart == null) {
      localPart = new Participant(host, port)
      participants.put(localParticipantId, localPart)
    }
    //启动选主服务
    leaderLatch = zkClient.leaderLatch(masterSelectorPath, localParticipantId)
    leaderLatch.addListener(new LeaderLatchListener() {
      override def isLeader: Unit = {
        SystemLog.create()
        debug(("msg", s"${localParticipantId} is master"))
        //主服务器
        if (!leaderSelectorPromise.isCompleted) {
          //第一次成功选主
          leaderSelectorPromise.success(localPart)
        } else {
          //master发生变更,slaver -> master
          becomeMaster()
        }
      }

      override def notLeader(): Unit = {
        SystemLog.create()
        //master发生变更,一般不会出现，除非master服务器出现问题,master -> slaver
        becomeSlaver()
      }
    })
    //等待选主完成，master和slaver等待逻辑不一样
    val master = Await.result[Participant](leaderSelectorPromise.future, Duration.Inf)
    info(("msg", s"${localParticipantId} leader selector complete"))
    //如果master是自己，向所有slaver发送注册请求
    if (master.getParticipantId() == localParticipantId) {
      becomeMaster()
    } else {
      info(("msg", "i am slaver"), ("local", localParticipantId), ("participants", participants.values()))
    }
  }

  override def stop(): Unit = {
    if (leaderLatch != null) {
      leaderLatch.close()
    }
    if (pingSchedule != null && !pingSchedule.isCancelled) {
      pingSchedule.cancel()
    }
    if (zkWatcher != null) {
      zkWatcher.close()
    }
    debug("stop resource manager")
  }

  private def becomeMaster(): Unit = {
    //等待所有slaver注册完成
    val cdl = new CountDownLatch(participants.size() - 1)
    for (par <- participants) {
      val participantId = par._1
      if (participantId != localParticipantId) {
        //master等待slaver注册的超时时间
        val time = configIntOrDefault(config, "horoscope.rm.register.timeout", 5)
        retryAsk(getLeaderSelectorActor(participantId), LeaderSelectorMessage(SystemLog.get()), 1,
          time seconds).onComplete {
          case Success(slaver) =>
            info(("msg", s"${slaver} register success"))
            cdl.countDown()
          case Failure(exception) =>
            participants.remove(participantId)
            exception.printStackTrace()
            error(("msg", exception.getMessage))
            cdl.countDown()
        }
      }
    }
    cdl.await()
    //发送最终的注册成功参与者
    for (par <- participants) {
      val participantId = par._1
      if (participantId != localParticipantId) {
        val participantId = par._2.getParticipantId()
        getLeaderSelectorActor(participantId) ! RegisterCompleteMessage(SystemLog.get(), local(),
          participants.values().toList)
      }
    }
    //监听zk,slaver注册
    zkWatcher = zkClient.watchNode(masterSelectorPath, SlaverListener)
    //加载scheduler数据
    scheduler.recovery(this)
    //开启ping
    ping()
    info(("msg", "i am master"), ("local", localParticipantId), ("participants", participants.values()))
  }

  private def becomeSlaver(): Unit = {
    //确定被选主slaver机器，确保master相关监控关闭
    if (pingSchedule != null && !pingSchedule.isCancelled) {
      pingSchedule.cancel()
    }
    if (zkWatcher != null) {
      zkWatcher.close()
    }
    debug("become slaver")
  }

  private def addParticipant(slaver: Participant): Unit = {
    participantLock.lock()
    try {
      if (reParticipants.size() > 0) {
        reParticipants.put(slaver.getParticipantId(), slaver)
      } else {
        for (v <- participants) {
          reParticipants.put(v._1, v._2)
        }
        reParticipants.put(slaver.getParticipantId(), slaver)
      }
      isRepartition.compareAndSet(false, true)
    } finally {
      participantLock.unlock()
    }
  }

  private def removePartition(participantId: String): Unit = {
    participantLock.lock()
    try {
      if (reParticipants.size() > 0) {
        reParticipants.remove(participantId)
      } else {
        for (v <- participants) {
          reParticipants.put(v._1, v._2)
        }
        reParticipants.remove(participantId)
      }
      isRepartition.compareAndSet(false, true)
    } finally {
      participantLock.unlock()
    }
  }

  //更新当前的所有参与主机
  private def resetParticipants(participants: List[Participant]): Unit = {
    participantLock.lock()
    try {
      val srcParticipants = this.participants.values().toList
      if ((participants diff srcParticipants).size == 0
        && (srcParticipants diff participants).size == 0
        && sourceRPCClients.size() != 0) {
        warn(("msg", "participants not any changed"),
          ("src participants", this.participants.values()),
          ("dest participants", participants),
          ("sourceRPCClients", sourceRPCClients.keys().toList))
      } else {
        this.participants.clear()
        for (participant <- participants) {
          this.participants.put(participant.getParticipantId(), participant)
        }
        //重新注册rpc clients
        for (sourceClients <- sourceRPCClients) {
          val source = sourceClients._1
          val sourceKey = lastSplit(source, SOURCEKEY_MARK)
          if (sourceKey.size == 2) {
            val sourceName = sourceKey(0)
            val params = sourceClients._2.params
            registerRPCClient(sourceName, params)
          } else {
            error(("msg", "source key error"), ("sourceKey", source))
          }
        }
        info(("msg", "reset participants complete"), ("participants", participants))
      }
    } finally {
      participantLock.unlock()
    }
  }

  private def resetParticipantsMessage(): Unit = {
    for (par <- reParticipants.values()) {
      if (par.getParticipantId() != localParticipantId) {
        getLeaderSelectorActor(par.getParticipantId()) !
          RegisterCompleteMessage(SystemLog.get(), local(), reParticipants.values().toList)
      } else {
        //master直接变更，slaver通过发消息变更
        resetParticipants(reParticipants.values().toList)
        scheduler.recovery(this)
      }
    }
  }

  object SlaverListener extends PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      SystemLog.create()
      event.getType match {
        case Type.CHILD_REMOVED =>
          val bytes: Array[Byte] = event.getData.getData
          val participantId: String = new String(bytes, "UTF-8")
          debug(("msg", "zk remove participant"), ("participant", participantId))
        case Type.CHILD_ADDED =>
          val bytes: Array[Byte] = event.getData.getData
          val participantId: String = new String(bytes, "UTF-8")
          //slaver注册
          val par = participants.get(participantId)
          debug(("msg", s"add participant ${participantId}"), ("participant", par))
          if (par == null) {
            //不在当前集群配置里(动态扩容)
            val hostPort = participantId.split(":")
            if (hostPort.size == 2) {
              val time = configIntOrDefault(config, "horoscope.rm.register.timeout", 5)
              retryAsk(getLeaderSelectorActor(participantId), LeaderSelectorMessage(SystemLog.get()), 1,
                time seconds).onComplete {
                case Success(slaver) =>
                  addParticipant(slaver.asInstanceOf[Participant])
                  debug(("msg", "add participant success"), ("slaver", slaver))
                case Failure(exception) =>
                  exception.printStackTrace()
                  error(("msg", "add new slaver error"), ("ex", exception.getMessage))
              }
            } else {
              error(("msg", "error participantId"), ("participantId", participantId))
            }
          } else {
            info(("msg", s"$participantId has register"))
            if (participantId != localParticipantId) {
              val attempts = configIntOrDefault(config, "horoscope.rm.restart.attempts", 15)
              val interval = configIntOrDefault(config, "horoscope.rm.restart.interval", 1)
              val restartMessage = SlaverRestartMessage(SystemLog.get(), local(), participants.values().toList)
              retryAsk(getLeaderSelectorActor(participantId), restartMessage, attempts, interval seconds).onComplete {
                case Success(slaver) =>
                  val restartPart = slaver.asInstanceOf[Participant]
                  if (!participants.containsKey(restartPart.getParticipantId())) {
                    //二次检查，进来时还在服务器列表中，restart结束后可能已经被摘除
                    addParticipant(restartPart)
                    info(("msg", "restart a slaver that was stopped"), ("participant", participantId))
                  } else {
                    info(("msg", "zk restart participant"), ("participant", participantId), ("result", slaver))
                  }
                case Failure(exception) =>
                  error(("msg", "zk remove participant"),
                    ("participant", participantId), ("ex", exception.getMessage))
              }
            }
          }
        case _ => {
          debug(("msg", "zookeeper child event"), ("type", event.getType))
        }
      }
    }
  }

  def ping(): Unit = {
    this.synchronized {
      if (pingSchedule == null || pingSchedule.isCancelled) {
        debug(("msg", "begin ping"), ("master", local().getParticipantId()))
        val systemScheduler = system.scheduler
        //每10秒执行一次
        val interval = configIntOrDefault(config, "horoscope.rm.ping.interval", 10)
        val schedule = systemScheduler.schedule(interval seconds, interval seconds, new Runnable {
          override def run(): Unit = {
            SystemLog.create()
            try {
              //需要ping参与者数量
              val cdl = new CountDownLatch(participants.size() - 1)
              for (participant <- participants.values()) {
                if (participant.getParticipantId() != localParticipantId) {
                  val participantId = participant.getParticipantId()
                  info(("msg", s"Ping ${participantId}"), ("participants", participants),
                    ("clients", sourceRPCClients), ("slots", s"${getSlotRange(local())}"))
                  //每个参与者最多等待30s，如果失败表示需要重新分区
                  val retryAttempts = configIntOrDefault(config, "horoscope.rm.ping.retry.attempts", 29)
                  val retryInterval = configIntOrDefault(config, "horoscope.rm.ping.retry.interval", 1)
                  retryAsk(getLeaderSelectorActor(participantId), PingMessage(SystemLog.get()), retryAttempts,
                    retryInterval seconds).onComplete {
                    case Success(result) =>
                      try {
                        if (!result.asInstanceOf[PongMessage].success) {
                          debug(("msg", "ping error"), ("participant", participant), ("result", result))
                          removePartition(participantId)
                        } else {
                          debug(("msg", "ping success"), ("result", result))
                        }
                      } finally {
                        cdl.countDown()
                      }
                    case Failure(failure) =>
                      try {
                        error(("msg", "ping error"), ("participant", participant), ("ex", failure.getMessage))
                        removePartition(participantId)
                      } finally {
                        cdl.countDown()
                      }
                  }
                }
              } //for end
              val timeout = configIntOrDefault(config, "horoscope.rm.ping.timeout", 35)
              cdl.await(timeout, TimeUnit.SECONDS)
              if (isRepartition.getAndSet(false)) {
                info(("msg", "ping reset participant"))
                resetParticipantsMessage()
              }
            } catch {
              case ex: Exception =>
                ex.printStackTrace()
                error(("msg", "ping running error"), ("ex", ex.getMessage))
            }
          } //run end
        })
        pingSchedule = schedule
      }
    }
  }

  class LeaderSelectorActor extends Actor {

    override def receive: Receive = {
      //选举完成
      case LeaderSelectorMessage(logId) =>
        SystemLog.set(logId)
        becomeSlaver()
        val localPar = local()
        info(("msg", "LeaderSelectorMessage"), ("local", localPar))
        sender() ! localPar

      //注册完成，或者重分区完成，需要重新加载分区数据
      case RegisterCompleteMessage(logId: String, master: Participant, participants: List[Participant]) =>
        SystemLog.set(logId)
        info(("msg", "RegisterCompleteMessage"), ("participants", participants))
        //1. 更新participants
        resetParticipants(participants)
        //2. scheduler recovery
        scheduler.recovery(DefaultResourceManager.this)
        //3. 开始运行
        if (!leaderSelectorPromise.isCompleted) {
          leaderSelectorPromise.success(master)
        }

      //临时掉线恢复
      case SlaverRestartMessage(logId: String, master: Participant, participants: List[Participant]) =>
        SystemLog.set(logId)
        info(("msg", "SlaverRestartMessage"), ("participants", participants))
        //1. 更新participants
        resetParticipants(participants)
        //2. scheduler recovery
        scheduler.recovery(DefaultResourceManager.this)
        //3. 开始运行
        if (!leaderSelectorPromise.isCompleted) {
          leaderSelectorPromise.success(master)
        }
        sender() ! local()

      //ping/pong
      case PingMessage(logId) =>
        SystemLog.set(logId)
        info(("msg", "PingMessage"), ("participants", participants), ("clients", sourceRPCClients),
          ("slots", s"${getSlotRange(local())}"))
        if (participants.size() == 0) {
          sender() ! PongMessage(logId, false)
        } else {
          sender() ! PongMessage(logId, true)
        }
      case msg: Any =>
        error(("msg", s"unknow message $msg"))
    }
  }

}

