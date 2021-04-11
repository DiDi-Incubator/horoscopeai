package com.didichuxing.horoscope.util

import com.didichuxing.horoscope.core.Flow
import com.didichuxing.horoscope.core.Flow.{Choice, Evaluate, Include, Node, Schedule, Subscribe, Terminator, _}
import com.didichuxing.horoscope.runtime.Value
import com.didichuxing.horoscope.runtime.convert.ValueTypeAdapter
import com.didichuxing.horoscope.util.FlowGraph._
import com.google.gson.{Gson, GsonBuilder}
import org.jgrapht.EdgeFactory
import org.jgrapht.alg.shortestpath.DijkstraShortestPath
import org.jgrapht.graph._

import scala.collection.JavaConversions._

class FlowGraph(vertices: Seq[FlowVertex], includes: Seq[IncludeEdge],
  subscribes: Seq[SubScribeEdge], schedules: Seq[ScheduleEdge]) {
  implicit val gson: Gson = new GsonBuilder()
    .registerTypeHierarchyAdapter(classOf[Value], new ValueTypeAdapter)
    .setPrettyPrinting()
    .serializeNulls()
    .create()

  private lazy val directedGraph: DefaultDirectedGraph[String, _] = {
    case class Edge(from: String, to: String)
    val graph = new DefaultDirectedGraph(new EdgeFactory[String, Edge] {
      override def createEdge(sourceVertex: String, targetVertex: String): Edge = Edge(sourceVertex, targetVertex)
    })
    vertices.foreach(v => graph.addVertex(v.name))
    (includes ++ subscribes ++ schedules).foreach(e => graph.addEdge(e.from, e.to))

    graph
  }

  private lazy val shortestPath = new DijkstraShortestPath(directedGraph)

  def getPath(from: String, to: String): Seq[String] = shortestPath.getPath(from, to).getVertexList

  def toJson: String = {
    Value(
      Map("flows" -> Value(vertices), "includes" -> Value(includes),
        "subscribes" -> Value(subscribes), "schedules" -> Value(schedules))
    ).toJson
  }
}

class FlowGraphBuilder(flows: Seq[Flow]) {

  def build(): FlowGraph = {
    val vertices: Seq[FlowVertex] = flows.map(flowToVertex)
    var includes: List[IncludeEdge] = Nil
    var schedules: List[ScheduleEdge] = Nil
    var subscribes: List[SubScribeEdge] = Nil

    flows.foreach { f =>
      val terminator = f.terminator
      val nodes = Node.search(terminator.deps) {
        case _: Include => Nil
        case _: Subscribe => Nil
        case _: Schedule => Nil
        case choice: Choice => choice.action :: choice.deps.toList
        case block: Terminator => block.deps
      }

      nodes.foreach {
        case include: Include => includes ::= IncludeEdge(f.name, include.flow, include.scope)
        case subscribe: Subscribe => subscribes ::= SubScribeEdge(f.name, subscribe.flow,
          subscribe.scope, subscribe.definition.condition,
          subscribe.definition.target, subscribe.definition.traffic
        )
        case schedule: Schedule => schedules ::= ScheduleEdge(f.name, schedule.flow, schedule.scope,
          schedule.waitTime.toString, schedule.trace.map(_.asInstanceOf[Evaluate].expression.toString()))
        case _ => Unit
      }
    }

    new FlowGraph(vertices, includes.distinct, subscribes.distinct, schedules.distinct)
  }
}

object FlowGraph {
  case class FlowVertex(name: String, args: Seq[String], experiment: Option[String])

  def flowToVertex(flow: Flow): FlowVertex = {
    FlowVertex(flow.name, flow.arguments.keys.toSeq, flow.flowConf.enabledExperiment)
  }

  trait Edge {
    def from: String

    def to: String
  }

  case class IncludeEdge(from: String, to: String, scope: String) extends Edge

  case class SubScribeEdge(from: String, to: String, scope: String,
    condition: Option[String], target: Option[String], traffic: Bucket) extends Edge

  case class ScheduleEdge(from: String, to: String, scope: String,
    waitTime: String,  traceId: Option[String]) extends Edge
}
