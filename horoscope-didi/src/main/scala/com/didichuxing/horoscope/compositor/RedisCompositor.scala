/*
 * Copyright (C) 2021 DiDi Inc. All Rights Reserved.
 * Authors: chenyiran@didiglobal.com
 * Description:
 */
package com.didichuxing.horoscope.compositor

import com.didichuxing.horoscope.core.{Compositor, CompositorFactory}
import com.didichuxing.horoscope.runtime.{Text, Value, ValueDict}
import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.{ConfigException, ConfigFactory}
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

import scala.concurrent.{ExecutionContext, Future}

class RedisCompositor(
  method: String, pool: JedisPool, expireSec: Int
)(implicit ec: ExecutionContext) extends Compositor with Logging {

  import RedisCompositor._
  import com.didichuxing.horoscope.runtime.Implicits.gson

  override def composite(args: ValueDict): Future[Value] = Future {
    assert(VALID_METHODS.contains(method.toLowerCase),
      s"unsupported redis method $method for ${VALID_METHODS.mkString(",")}")
    val keyOpt = args.at("key")
    assert(keyOpt.nonEmpty, "redis compositor need key field")
    val key = keyOpt.get.as[Text].underlying
    val jedis = pool.getResource
    try {
      method match {
        case "get" => gson.fromJson(jedis.get(key), classOf[Value])
        case "set" =>
          val valueOpt = args.at("value")
          assert(valueOpt.nonEmpty, "redis compositor need value field")
          val value = valueOpt.get.toJson
          val result = Value(jedis.set(key, value))
          jedis.expire(key, expireSec)
          result
      }
    } finally {
      jedis.close()
    }
  }
}

object RedisCompositor {
  private val VALID_METHODS = Set("get", "set")
}

case class RedisCompositorException(message: String, cause: Option[Throwable] = None)
  extends Exception(message, cause.orNull)

class RedisCompositorFactory(implicit ec: ExecutionContext) extends CompositorFactory with Logging {

  import RedisCompositorFactory._

  override def name(): String = "redis"

  override def create(code: String)(resource: String => Array[Byte]): Compositor = {
    try {
      val config = ConfigFactory.parseString(code)
      val method = config.getString("method")
      val host = config.getString("host")
      val port = config.getInt("port")
      val expireSec = try {
        config.getInt("expire-sec")
      } catch {
        case _: ConfigException.Missing => 90 * 86400
      }

      info(s"create jedis pool: $host:$port")
      val pool = createJedisPool(host, port)
      new RedisCompositor(method, pool, expireSec)
    } catch {
      case e: Exception =>
        throw RedisCompositorException(s"Invalid redis compositor config $code", Some(e.getCause))
    }
  }
}

object RedisCompositorFactory {
  private def createJedisPool(host: String, port: Int) = {
    val jedisConfig = new JedisPoolConfig()
    jedisConfig.setMaxTotal(-1)
    jedisConfig.setMaxIdle(100)
    jedisConfig.setMinIdle(10)
    new JedisPool(jedisConfig, host, port, 20000)
  }
}
