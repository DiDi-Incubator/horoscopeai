/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.util

import java.io._
import java.net.{Inet4Address, NetworkInterface}
import java.util.UUID
import java.util.zip.CRC32

import akka.actor.ActorSelection
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.StandardRoute
import akka.util.Timeout
import akka.pattern.ask
import com.didichuxing.horoscope.runtime.{Binary, NumberValue, Text, Value}
import com.typesafe.config.Config
import org.apache.hadoop.hbase.util.Bytes

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConversions._

object Utils extends Logging {

  System.setProperty("java.util.secureRandomSeed", "true")

  /**
   * 重试请求，重试过程中不回调
   *
   * @param actor
   * @param message
   * @param attempts
   * @param interval
   * @param executor
   * @return
   */
  def retryAsk(actor: ActorSelection, message: Any, attempts: Int, interval: FiniteDuration)
              (implicit executor: ExecutionContext): Future[Any] = {
    val promise = Promise[Any]
    //请求必须1秒内返回，否则会触发重试
    actor.ask(message)(Timeout(interval)).onComplete {
      case Success(value) =>
        promise.success(value)
      case Failure(exception) =>
        if (attempts > 0) {
          //Thread.sleep(interval.toMillis)
          retryAsk(actor, message, attempts - 1, interval).onComplete {
            case Success(value) =>
              promise.success(value)
            case Failure(exception) =>
              promise.failure(exception)
          }
        } else {
          promise.failure(exception)
        }
    }
    promise.future
  }

  /**
   * 获取本机ip
   *
   * @return
   */
  def guessLocalIp(): String = {
    var guessIP = "127.0.0.1"
    val allNetInterfaces = NetworkInterface.getNetworkInterfaces()
    for (netInterface <- allNetInterfaces) {
      if (netInterface.isUp && !netInterface.isLoopback && !netInterface.isVirtual) {
        val addresses = netInterface.getInetAddresses
        for (address <- addresses) {
          if (address != null && address.isInstanceOf[Inet4Address]) {
            guessIP = address.getHostAddress
          }
        }
      }
    }
    guessIP
  }

  /**
   * 根据traceId计算出slot
   *
   * @param traceId
   * @return
   */
  def getSlot(traceId: String, slotCount: Int): Int = {
    val crc32 = new CRC32()
    crc32.reset()
    crc32.update(Bytes.toBytes(traceId))
    val hash = crc32.getValue
    val slot = hash % slotCount
    slot.toInt
  }

  def getClusterSlotCount(config: Config): Int = {
    Try(config.getInt("horoscope.rm.slot-count")).getOrElse(Constants.CLUSTER_SLOT_COUNT)
  }

  def getSlotBatchSize(config: Config): Int = {
    Try(config.getInt("horoscope.rm.slot-batch")).getOrElse(Constants.SCH_BATCH_SIZE)
  }

  /**
   * 生成唯一eventId
   *
   * @return
   */
  def getEventId(): String = {
    //UUID.randomUUID().toString.replace("-", "")
    ObjectID.next
  }

  /**
   * 生成唯一traceId
   *
   * @param eventId
   * @return
   */
  def getTraceId(eventId: String): String = {
    UUID.nameUUIDFromBytes(eventId.getBytes).toString.replace("-", "")
  }

  /**
   * config int value or default
   *
   * @param config
   * @param path
   * @param default
   * @return
   */
  def configIntOrDefault(config: Config, path: String, default: Int): Int = {
    try {
      config.getInt(path)
    } catch {
      case ex: Exception =>
        debug(("msg", ex.getMessage))
        default
    }
  }

  def close(closeable: Closeable): Unit = {
    if (closeable != null) {
      try {
        closeable.close();
      } catch {
        case ex: Exception =>
          ex.printStackTrace()
      }
    }
  }

  def lastSplit(unSplit: String, tag: String): Array[String] = {
    val idx = unSplit.lastIndexOf(tag)
    if (idx == -1) {
      Array[String](unSplit)
    } else {
      val x = unSplit.substring(0, idx)
      val y = unSplit.substring(idx + 1, unSplit.length)
      Array[String](x, y)
    }
  }

  /**
   * trace store 中存储gotoEvent的column
   */
  def schStoreCol(config: Config): String = {
    Try(config.getString("horoscope.scheduler.store-col")).getOrElse("_SCD_")
  }

  def printStackTraceStr(cause: Throwable): String = {
    val sw: StringWriter = new StringWriter()
    val pw: PrintWriter = new PrintWriter(sw)
    cause.printStackTrace(pw)
    sw.toString()
  }

  def bucket(value: Value, suffix: Option[String] = None): Int = {
    val uuid = value match {
      case Text(text) => UUID.nameUUIDFromBytes(text.getBytes).toString
      case NumberValue(v) => UUID.nameUUIDFromBytes(v.toDouble.toString.getBytes).toString
      case Binary(bytes) => UUID.nameUUIDFromBytes(bytes).toString
      case _ => UUID.randomUUID().toString
    }

    val slot = if (suffix.isDefined) {
      Utils.getSlot(uuid + suffix,100)
    } else {
      Utils.getSlot(uuid,100)
    }

    slot
  }

  def run(block: => Unit): StandardRoute = {
    import akka.http.scaladsl.server.Directives._
    try {
      block
      complete(StatusCodes.OK)
    } catch {
      case e: Throwable =>
        complete(StatusCodes.NotAcceptable, e.getMessage)
    }
  }
}
