package com.didichuxing.horoscope.core

import com.didichuxing.horoscope.core.ExperimentController._
import com.didichuxing.horoscope.core.FlowRuntimeMessage._
import com.didichuxing.horoscope.runtime.expression.{BuiltIn, Expression}
import com.didichuxing.horoscope.runtime.{Value, ValueDict}
import com.typesafe.config.Config

import scala.collection.JavaConverters._

trait ExperimentControllerFactory {
  def name: String

  def create(config: Config)(builtIn: BuiltIn): ExperimentController
}

// The experiment controller is one-to-one with the experiment conf.
trait ExperimentController {

  def query(args: ValueDict): Option[ExperimentPlan]

  def dependency: Map[String, Expression]

  def priority: Int
}

object ExperimentController {
  case class FlowOption(replacement: String, params: Map[String, String])
  case class ExperimentPlan(name: String, group: String, flowOptions: Map[String, FlowOption])

  implicit class ExperimentPlanHelper(plan: ExperimentPlan) {
    def experimentContext: ExperimentContext = {
      val builder = ExperimentContext.newBuilder()
        .setName(plan.name).setGroup(plan.group)
      for ((original, flowOption) <- plan.flowOptions) {
        val args = flowOption.params.mapValues(v => Value(v).as[FlowValue])
        val flowPlanBuilder = ExperimentContext.Alternative.newBuilder()
          .setOriginal(original)
          .setReplacement(flowOption.replacement)
          .putAllArgument(args.asJava)
        builder.addPlan(flowPlanBuilder.build())
      }
      builder.build()
    }
  }
}
