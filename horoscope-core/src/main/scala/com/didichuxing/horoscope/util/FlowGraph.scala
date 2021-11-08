package com.didichuxing.horoscope.util

import java.util.concurrent.TimeUnit

import com.didichuxing.horoscope.core.Flow
import com.didichuxing.horoscope.core.Flow._
import com.didichuxing.horoscope.runtime.Value
import com.didichuxing.horoscope.runtime.convert.ValueTypeAdapter
import com.didichuxing.horoscope.util.FlowGraph._
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.gson.{Gson, GsonBuilder}
import org.jgrapht.EdgeFactory
import org.jgrapht.alg.shortestpath.DijkstraShortestPath
import org.jgrapht.graph._

import scala.collection.JavaConversions._

class FlowGraph(vertices: Seq[FlowVertex], edges: Seq[Edge]) extends Logging {
  implicit val gson: Gson = new GsonBuilder()
    .registerTypeHierarchyAdapter(classOf[Value], new ValueTypeAdapter)
    .setPrettyPrinting()
    .serializeNulls()
    .create()

  private lazy val directedGraph: DefaultDirectedGraph[String, Edge] = {
    val graph = new DefaultDirectedGraph(new EdgeFactory[String, Edge] {
      override def createEdge(sourceVertex: String, targetVertex: String): Edge = null
    })
    vertices.foreach(v => graph.addVertex(v.name))
    edges.foreach { e =>
      if (graph.containsVertex(e.from) && graph.containsVertex(e.to)) {
        graph.addEdge(e.from, e.to, e)
      }
    }

    graph
  }

  private lazy val shortestPath = new DijkstraShortestPath(directedGraph)

  private val pathCache: LoadingCache[(String, String), Seq[String]] = CacheBuilder.newBuilder()
    .maximumSize(1000)
    .concurrencyLevel(128)
    .expireAfterAccess(2, TimeUnit.HOURS
    ).build(
    new CacheLoader[(String, String), Seq[String]] {
      def load(key: (String, String)): Seq[String] = {
        val graphPath = shortestPath.getPath(key._1, key._2)
        if (graphPath == null) {
          Nil
        } else {
          graphPath.getVertexList
        }
      }
    }
  )

  def getPath(from: String, to: String): Seq[String] = try {
    pathCache.get((from, to))
  } catch {
    case e: Throwable =>
      error(s"calc path failed from flow: ${from}, to: ${to}, message: ${e.getMessage}")
      Nil
  }

  def hasPath(from: String, to: String): Boolean = getPath(from, to).nonEmpty

  private val callbackDuals: Map[String, String] = {
    edges.filter(_.`type` == "callback").map(r => (r.to, r.options("timeoutFlow"))).toMap ++
      edges.filter(_.`type` == "callback").map(r => (r.options("timeoutFlow"), r.to)).toMap
  }

  def getDualFlow(flow: String): Option[String] = callbackDuals.get(flow)

  def toJson: String = {
    Value(
      Map("flows" -> Value(vertices),
        "includes" -> Value(edges.filter(_.`type` == "include")),
        "subscribes" -> Value(edges.filter(_.`type` == "subscribe")),
        "schedules" -> Value(edges.filter(_.`type` == "schedule")),
        "callbacks" -> Value(edges.filter(_.`type` == "callback")))
    ).toJson
  }
}

object FlowGraph {
  case class FlowVertex(name: String, args: Seq[String], experiment: Seq[String] = Nil)

  object FlowVertex {
    def apply(flow: Flow): FlowVertex = {
      val experiments = flow.flowConf.experiments.map(_.getString("name"))
      FlowVertex(flow.name, flow.arguments.keys.toSeq, experiments)
    }
  }

  case class Edge(from: String, to: String, scope: String, `type`: String, options: Map[String, String] = Map.empty)

  object Edge {
    def apply(flow: String, include: Include): Edge = {
      Edge(flow, include.flow, include.scope, `type` = "include")
    }

    def apply(flow: String, schedule: Schedule): Edge = {
      Edge(flow, schedule.flow, schedule.scope, `type` = "schedule",
        options = Map("waitTime" -> schedule.waitTime.toString) ++
          schedule.trace.map(_.asInstanceOf[Evaluate].expression.toString()).map("trace" -> _)
      )
    }

    def apply(flow: String, subscribe: Subscribe): Edge = {
      val definition = subscribe.definition
      Edge(flow, subscribe.flow, subscribe.scope, `type` = "subscribe",
        options = Map("traffic" -> definition.traffic.toString()) ++
          definition.condition.map("condition" -> _) ++
          definition.target.map("target" -> _)
      )
    }

    def apply(flow: String, callback: Callback): Edge = {
      Edge(flow, callback.flow, callback.scope, `type` = "callback",
        options = Map(
          "timeout" -> callback.timeout.toString,
          "token" -> callback.definition.token,
          "timeoutFlow" -> callback.timeoutFlow
        )
      )
    }
  }

  def newBuilder(flows: Seq[Flow]): FlowGraphBuilder = {
    new FlowGraphBuilder(flows)
  }

  class FlowGraphBuilder(flows: Seq[Flow]) {

    def build(): FlowGraph = {
      val vertices: Seq[FlowVertex] = flows.map(FlowVertex(_))
      var edges: List[Edge] = Nil
      flows.foreach { f =>
        val terminator = f.terminator
        val nodes = Node.search(terminator.deps) {
          case _: Include => Nil
          case _: Subscribe => Nil
          case _: Schedule => Nil
          case _: Callback => Nil
          case choice: Choice => choice.action :: choice.deps.toList
          case block: Terminator => block.deps
        }
        nodes.foreach {
          case include: Include => edges ::= Edge(f.name, include)
          case subscribe: Subscribe => edges ::= Edge(f.name, subscribe)
          case schedule: Schedule => edges ::= Edge(f.name, schedule)
          case callback: Callback => edges ::= Edge(f.name, callback)
          case _ => Unit
        }
      }

      new FlowGraph(vertices, edges.distinct)
    }
  }
}
