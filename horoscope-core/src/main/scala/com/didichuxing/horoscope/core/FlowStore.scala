package com.didichuxing.horoscope.core

import akka.http.scaladsl.server.Route
import com.didichuxing.horoscope.runtime.expression.BuiltIn
import com.didichuxing.horoscope.util.FlowGraph

trait FlowStore {
  def getFlow(name: String): Flow = throw new NotImplementedError()

  def getController(name: String): Seq[ExperimentController] = throw new NotImplementedError()

  def getFlowGraph: FlowGraph = throw new NotImplementedError()

  def getBuiltIn: BuiltIn

  def api: Route = _.reject()
}
