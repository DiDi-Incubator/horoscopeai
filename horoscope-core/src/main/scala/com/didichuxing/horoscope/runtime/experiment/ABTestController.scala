package com.didichuxing.horoscope.runtime.experiment

import com.didichuxing.horoscope.core.FlowConf.Bucket
import com.didichuxing.horoscope.runtime.experiment.ExperimentController._
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

  private val exptConf = conf.parseABTestConf()

  def dependency: Map[String, Expression] = {
    (exptConf.condition ++ exptConf.target ++
      exptConf.groups.flatMap(_.flows.flatMap(_._2.params.values))
      ).flatMap(Expression(_).references).map(ref => ref.name -> ref).toMap
  }

  override def query(args: ValueDict): Option[ExperimentPlan] = {
    try {
      val traffic = exptConf.traffic
      val condition = exptConf.condition.map(Expression(_).apply(args))
      val bucketId = Utils.bucket(Expression("@event_id").apply(args))
      // condition是空时，默认满足条件
      if (condition.forall(_.as[Boolean]) && traffic.contains(bucketId)) {
        val target = (exptConf.target orElse Some("@event_id")).map(Expression(_).apply(args))
        val targetBucket = Utils.bucket(target.get, Some(exptConf.name))
        val satisfied = exptConf.groups.find(_.traffic.contains(targetBucket)).get
        Some(ExperimentPlan(exptConf.name, satisfied.group, satisfied.flows))
      } else {
        None
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

  override def priority: Int = exptConf.priority
}

object ABTestController {
  case class ABTestConf(name: String, enabled: Boolean, flow: String,
    condition: Option[String], target: Option[String], groups: Seq[ABTestGroup],
    priority: Int, traffic: Bucket) {
    def expressions: Map[String, Seq[String]] = {
      Map(flow -> (condition ++ target).toSeq)
    }
  }

  // flows: originalFlow -> FlowOption
  case class ABTestGroup(group: String, traffic: Bucket, flows: Map[String, FlowOption])
}
