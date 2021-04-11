package com.didichuxing.horoscope.runtime.experiment

import com.didichuxing.horoscope.core.Flow.Bucket
import com.didichuxing.horoscope.runtime.experiment.ExperimentController.ExperimentChoice
import com.didichuxing.horoscope.runtime.expression.{BuiltIn, Expression}
import com.didichuxing.horoscope.runtime.{Value, ValueDict}
import com.didichuxing.horoscope.util.FlowConfParser._
import com.didichuxing.horoscope.util.{Logging, Utils}
import com.typesafe.config.Config

class ABTestControllerFactory() extends ControllerFactory {
  override def name: String = "ab_test"

  override def create(config: Config)(builtIn: BuiltIn): ExperimentController = {
    new ABTestController(config)(builtIn)
  }
}

class ABTestController(conf: Config)(
  implicit builtIn: BuiltIn
) extends ExperimentController with Logging {

  private val DEFAULT_GROUP = "default_group"
  private val exptConf = conf.parseABTestConf()

  def dependency: Map[String, Expression] = {
    (exptConf.condition ++ exptConf.target).flatMap(Expression(_).references).map(ref => ref.name -> ref).toMap
  }

  override def evaluate(args: ValueDict): ExperimentChoice = {
    try {
      val condition = exptConf.condition.map(Expression(_).apply(args))
      val target = (exptConf.target orElse Some("@event_id")).map(Expression(_).apply(args))
      if (condition.forall(!_.as[Boolean])) {
        // default flow
        ExperimentChoice(exptConf.name, DEFAULT_GROUP, exptConf.flow, Map())
      } else {
        val hit = exptConf.groups.find(_.traffic.contains(Utils.bucket(target.get))).get
        ExperimentChoice(exptConf.name, hit.group, hit.flow, hit.params)
      }
    } catch {
      case cause: Throwable =>
        val msg =
          s"""evaluate experiment failed, name: ${exptConf.name}, flow: ${exptConf.flow},
             | cause: ${cause.getMessage}""".stripMargin
        error(msg)
        throw new RuntimeException(msg, cause)
    }
  }
}

object ABTestController {
  case class ABTestConf(name: String, enabled: Boolean, flow: String,
    condition: Option[String], target: Option[String], groups: Seq[ABTestGroup])

  case class ABTestGroup(group: String, flow: String, params: Map[String, Value], traffic: Bucket)
}
