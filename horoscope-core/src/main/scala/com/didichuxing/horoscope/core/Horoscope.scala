package com.didichuxing.horoscope.core

import com.didichuxing.horoscope.service.local.LocalServiceBuilder

object Horoscope {
  def newLocalService(): LocalServiceBuilder = new LocalServiceBuilder()
}
