package com.didichuxing.horoscope.core

import com.didichuxing.horoscope.runtime.{Value, ValueDict}

import scala.concurrent.Future

trait Compositor {
  def composite(args: ValueDict): Future[Value]
}

trait CompositorFactory extends Serializable {
  def name(): String

  def create(code: String)(resource: String => Array[Byte]): Compositor
}

object Compositor {
  def failed(cause: Throwable): Compositor = new Compositor {
    def composite(args: ValueDict): Future[Value] = {
      Future.failed(cause)
    }
  }
}
