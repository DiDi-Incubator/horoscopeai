package com.didichuxing.horoscope.logging

import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowInstance
import com.didichuxing.horoscope.core.OdsLogger
import com.didichuxing.horoscope.util.PublicLog

class LocalProcedureViewLogger(publicLog: PublicLog) extends OdsLogger {
  import com.didichuxing.horoscope.logging.Implicits._
  override def log(flowInstance: FlowInstance): Unit = {
    val procedureViews = ProcedureViewBuilder.buildFrom(flowInstance)
    procedureViews.foreach { record =>
      publicLog.public(("procedure_view", record.toJson()))
    }
  }
}
