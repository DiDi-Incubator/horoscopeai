/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.compositor

import java.io.File
import java.util.concurrent.{Callable, ScheduledExecutorService, TimeUnit}
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.didichuxing.horoscope.core.{Compositor, CompositorFactory}
import com.didichuxing.horoscope.runtime.{Binary, BooleanValue, NULL, NumberValue, Text, Value, ValueDict, ValueList}
import com.didichuxing.horoscope.util.Logging
import com.google.common.cache.{Cache, CacheBuilder}
import com.google.common.io.Resources
import com.typesafe.config.{Config, ConfigFactory}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.io.Source
import scala.util.{Failure, Success, Try}

case class WrapValue(value: Value = NULL)

class GraphQLCompositor(url: String, query: String, config: Config)
  (implicit actorSystem: ActorSystem,
    actorMaterializer: ActorMaterializer,
    executionContext: ExecutionContext,
    scheduleExecutor: ScheduledExecutorService) extends RestfulClientHelper with Compositor {

  val cacheKey = Try{ config.getString("cache.key") }.getOrElse("")
  val cacheTTL = Try{ config.getInt("cache.ttl") }.getOrElse(60)
  val cache: Cache[String, WrapValue] = CacheBuilder.newBuilder()
    .maximumSize(100000)
    .concurrencyLevel(128)
    .expireAfterAccess(cacheTTL, TimeUnit.SECONDS)
    .build()

  override def composite(args: ValueDict): Future[Value] = {
    val p = Promise[Value]()
    Future {
      debug(s"composite args=$args")
      if (cacheKey.nonEmpty) {
        debug(s"cache.key=$cacheKey, cache.entries.size=${cache.size()}")
        try {
          val keyValue = getKeyValue(args)
          val wrapValue = cache.getIfPresent(keyValue)
          info(s"cache.keyValue=$keyValue, cache.wrapValue=$wrapValue")
          if (wrapValue != null) {
            p.success(wrapValue.value)
          } else {
            graphQuery(args, p)
          }
        } catch {
          case e: Exception =>
            error(s"Get data from cache error, msg: ${e.getMessage}")
            graphQuery(args, p)
        }
      } else {
        graphQuery(args, p)
      }
    }
    p.future
  }

  /** 缓存中keyValue类型仅支持数字、字符串 */
  def getKeyValue(args: ValueDict): String = {
    val cacheKeyValue = args.visit(cacheKey)
    cacheKeyValue match {
      case v: NumberValue => v.toString
      case v: Text => v.underlying
      case _ => throw new ClassCastException(s"Cache key type does not support ${cacheKeyValue.valueType}," +
        s"only NumberValue/Text, cacheKeyValue: $cacheKeyValue")
    }
  }

  /** 调用graphql服务查询数据，并根绝查询到的'非空'数据设置cache */
  def graphQuery(args: ValueDict, promise: Promise[Value]): Unit = {
    val ql = DefaultGraphQLQueryStore.getGraphQL(query)
    if (ql.isEmpty) {
      error(("msg", "query not found"), ("query name", query))
      promise.failure(new Exception(s"query '${query}' not found"))
    } else {
      debug(("msg", "graphQL query"), ("query", ql.get))
      val body = Value(Map("query" -> ql.get))
      doPost(url)(body.updated("variables", args)).onComplete {
        case Success(value) =>
          try {
            debug(("msg", "graphQL query data"), ("data", value.toJson))
            if(value.isInstanceOf[ValueDict]) {
              val dict = value.asInstanceOf[ValueDict]
              val errors = dict.at("errors")
              if(errors.isEmpty) {
                val queryData = dict.visit("data")
                debug(s"graphQL query result, queryData: $queryData, dataIsEmpty: ${isEmpty(queryData)}")
                if (cacheKey.nonEmpty && ! isEmpty(queryData)) {
                  val keyValue = getKeyValue(args)
                  cache.put(keyValue, WrapValue(queryData))
                  debug(s"put entry to cache, key: $keyValue, data: $queryData")
                }
                promise.success(queryData)
              } else {
                error(("msg", "graphql error"), ("errors", errors))
                promise.failure(new Exception(s"$errors"))
              }
            } else {
              promise.failure(new Exception(s"the value must be ValueDict type : ${value.toJson}"))
            }
          } catch {
            case e: Exception =>
              error(s"Get data from graphQL error, msg: ${e.getMessage}")
              promise.failure(e)
          }
        case Failure(exception) =>
          error(("msg", "graphql post error"), ("ex", exception.getCause))
          promise.failure(exception)
      }
    }
  }

  /** 判断Value类型对象是否为空 */
  def isEmpty(value: Value): Boolean = value match {
    case NULL => true
    case v: Binary => v.length == 0
    case v: ValueList => {
      ! v.iterator.exists(vl => {
        ! isEmpty(vl._2)
      })
    }
    case v: ValueDict => {
      ! v.iterator.exists(vl => {
        ! isEmpty(vl._2)
      })
    }
    case _ => false
  }

}

class GraphQLCompositorFactory(compositorConfig: Config)
  (implicit actorSystem: ActorSystem,
    actorMaterializer: ActorMaterializer,
    executionContext: ExecutionContext,
    scheduleExecutor: ScheduledExecutorService) extends CompositorFactory with Logging{

  override def name(): String = {
    "graphql"
  }

  override def create(code: String): Compositor = {
    val flowConfig = CompositorUtil.parseConfigFromFlow(code)
    val restfulConfig = new RestfulCompositorConfig(compositorConfig, flowConfig)
    val url = restfulConfig.getServiceUrl
    val query = restfulConfig.getModelName
    // cache config Parse
    val config = compositorConfig.withFallback(parseCacheConfig(code))
    new GraphQLCompositor(url, query, config)
  }

  def parseCacheConfig(code: String): Config = {
    val codes = code.replaceAll("\\$|\\{|\\}", "").split("\n").filter(el => {
      val line = el.toLowerCase()
      ! (line.startsWith("post") || line.startsWith("get"))
    })
    ConfigFactory.parseString(codes.mkString("\n"))
  }

}

trait GraphQLQueryStore {
  def getGraphQL(query: String): Option[String]
}

object DefaultGraphQLQueryStore extends GraphQLQueryStore with Logging {

  private val rootPath = Resources.getResource("graphql").getFile
  private val root = new File(rootPath)
  private val fileList = root.listFiles().filter(_.isFile)
  private val graphQLList: Map[String, String] = fileList
    .map(file => {
      val filename = file.getName
      info(("msg", s"load graphql def ${filename}"))
      val content = Source.fromFile(file.getAbsolutePath).getLines.mkString
      filename -> content
    }).toMap

  override def getGraphQL(query: String): Option[String] = {
    graphQLList.get(query)
  }
}
