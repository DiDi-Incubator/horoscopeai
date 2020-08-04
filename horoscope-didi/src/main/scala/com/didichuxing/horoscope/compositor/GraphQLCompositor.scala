/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.compositor

import java.io.File
import java.util.concurrent.ScheduledExecutorService

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.didichuxing.horoscope.core.{Compositor, CompositorFactory}
import com.didichuxing.horoscope.runtime.{Value, ValueDict}
import com.didichuxing.horoscope.util.Logging
import com.google.common.io.Resources
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.io.Source
import scala.util.{Failure, Success}

class GraphQLCompositor(url: String, query: String)
                       (implicit actorSystem: ActorSystem,
                        actorMaterializer: ActorMaterializer,
                        executionContext: ExecutionContext,
                        scheduleExecutor: ScheduledExecutorService) extends RestfulClientHelper with Compositor {

  override def composite(args: ValueDict): Future[Value] = {
    val p = Promise[Value]()
    Future {
      val ql = DefaultGraphQLQueryStore.getGraphQL(query)
      if (ql.isEmpty) {
        error(("msg", "query not found"), ("query name", query))
        p.failure(new Exception(s"query '${query}' not found"))
      } else {
        debug(("msg", "graphQL query"), ("query", ql.get))
        val body = Value(Map("query" -> ql.get))
        doPost(url)(body.updated("variables", args)).onComplete {
          case Success(value) =>
            debug(("msg", "graphQL query data"), ("data", value.toJson))
            if(value.isInstanceOf[ValueDict]) {
              val dict = value.asInstanceOf[ValueDict]
              val errors = dict.at("errors")
              if(errors.isEmpty) {
                p.success(dict.visit("data"))
              } else {
                error(("msg", "graphql error"), ("errors", errors))
                p.failure(new Exception(s"$errors"))
              }
            } else {
              p.failure(new Exception(s"the value must be ValueDict type : ${value.toJson}"))
            }
          case Failure(exception) =>
            error(("msg", "graphql post error"), ("ex", exception.getCause))
            p.failure(exception)
        }
      }
    }
    p.future
  }

}

class GraphQLCompositorFactory(compositorConfig: Config)
                              (implicit actorSystem: ActorSystem,
                               actorMaterializer: ActorMaterializer,
                               executionContext: ExecutionContext,
                               scheduleExecutor: ScheduledExecutorService) extends CompositorFactory {

  override def name(): String = {
    "graphql"
  }

  override def create(code: String): Compositor = {
    val flowConfig = CompositorUtil.parseConfigFromFlow(code)
    val restfulConfig = new RestfulCompositorConfig(compositorConfig, flowConfig)
    val url = restfulConfig.getServiceUrl
    val query = restfulConfig.getModelName
    new GraphQLCompositor(url, query)
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
