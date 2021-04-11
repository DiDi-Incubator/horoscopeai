package com.didichuxing.horoscope.runtime.experiment

import com.didichuxing.horoscope.runtime.experiment.ExperimentController.ExperimentChoice
import com.didichuxing.horoscope.runtime.{Value, ValueDict}
import com.didichuxing.horoscope.runtime.expression.{BuiltIn, Expression}
import com.typesafe.config.Config

trait ControllerFactory {
  def name: String

  def create(config: Config)(builtIn: BuiltIn): ExperimentController
}

trait ExperimentController {
  def evaluate(args: ValueDict): ExperimentChoice

  def dependency: Map[String, Expression]
}

object ExperimentController {

  case class ExperimentChoice(name: String, group: String, flow: String, params: Map[String, Value])

}
