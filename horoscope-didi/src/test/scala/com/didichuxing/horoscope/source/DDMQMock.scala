/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.source

import java.util

import com.google.gson.GsonBuilder
import com.xiaojukeji.carrera.config.CarreraConfig
import com.xiaojukeji.carrera.producer.CarreraProducer
import com.xiaojukeji.carrera.sd.Env

class DDMQMock {

  def mock(): Unit = {
    val gson = new GsonBuilder().create()
    case class SourceEvent(traceId: String, eventId: String, value: String)

    val config = new CarreraConfig(Env.Test)
    config.setCarreraProxyTimeout(50)
    config.setCarreraClientTimeout(100)
    config.setCarreraClientRetry(2)
    config.setCarreraPoolSize(20)
    config.setBatchSendThreadNumber(16)
    val producer = new CarreraProducer(util.Arrays.asList("horoscope_source"), config)
    producer.start()
    for(i <- 0 to 1) {
      val event = SourceEvent(null, null, "value")
      val json = gson.toJson(event)
      producer.send("horoscope_source", json)
      //producer.send("horoscope_source", "json")
      Thread.sleep(1000)
    }
    producer.shutdown()
  }

}