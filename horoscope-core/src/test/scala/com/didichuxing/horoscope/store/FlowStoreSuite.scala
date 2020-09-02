/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.store

import com.didichuxing.horoscope.core.{Compositor, Flow, FlowDslMessage}
import com.didichuxing.horoscope.core.FlowDslMessage.{Block, CompositorDef, FlowDef}
import com.didichuxing.horoscope.runtime.{NULL, Value, ValueDict}
import com.didichuxing.horoscope.service.storage.DefaultFlowStore
import com.didichuxing.horoscope.util.{FlowChart, Logging}
import org.scalatest.{BeforeAndAfter, FunSuite}
import com.google.protobuf.util.JsonFormat

import scala.concurrent.Future

class FlowStoreSuite extends FunSuite with BeforeAndAfter with Logging {

  test("flow def") {
    val body = Block.newBuilder().build()
    val flow = FlowDslMessage.FlowDef.newBuilder().setId("f1").setName("/root/flow1").setBody(body).build()
    val json = JsonFormat.printer.print(flow)
    debug(("json", json))
    val flowBuilder = FlowDef.newBuilder()
    JsonFormat.parser.merge(json, flowBuilder)
    debug(("flow", flowBuilder))
  }

  ignore("flow store") {
    val flowStore = new DefaultFlowStore
    val flowDef = flowStore.getFlowByName("/traffic/road-close/original-intelligence")
    info(flowDef.toString)

    import com.didichuxing.horoscope.runtime.Implicits._
    implicit def buildCompositor(compositorDef: CompositorDef): Compositor = {
      new Compositor {
        override def composite(args: ValueDict): Future[Value] = Future.successful(NULL)
      }
    }

    val flow = Flow(flowDef)
    val chart = new FlowChart(flow)
    chart.show()
  }
}
