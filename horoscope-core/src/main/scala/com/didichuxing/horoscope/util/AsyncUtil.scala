package com.didichuxing.horoscope.util

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

object AsyncUtil extends Logging {
  def retry[T](retryLimits: Long, retryIntervalInMs: Long, retryCount: Long = 1)(
    func: => Future[T])(implicit executor: ExecutionContext, scheduleExecutor: ScheduledExecutorService): Future[T] = {
    require(retryLimits >= 0)
    func.recoverWith({
      case e: IgnoredException =>
        Future.failed(e)
      case e: Exception =>
      if (retryCount <= retryLimits) {
        logging.info(s"do action failed($retryCount/$retryLimits), retrying ${retryIntervalInMs}ms later")
        val p = Promise[T]
        scheduleExecutor.schedule(new Runnable {
          override def run(): Unit = {
            p.completeWith(retry(retryLimits, retryIntervalInMs * 2, retryCount + 1)(func))
          }
        }, retryIntervalInMs, TimeUnit.MILLISECONDS)
        p.future
      } else {
        Future.failed(e)
      }
    })
  }

  implicit class FutureAwait(future: Future[Any]) {
    def await(duration: Duration = Duration(10000, TimeUnit.MILLISECONDS)): Any = {
      Await.result(future, duration)
    }
  }

  case class IgnoredException(
    message: String, cause: Option[Throwable] = None
  ) extends Exception(message, cause.orNull)
}
