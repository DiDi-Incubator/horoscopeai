/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime

import java.util.concurrent.{Callable, TimeUnit}

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, ActorSystem, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy}
import com.didichuxing.horoscope.core.FlowRuntimeMessage._
import com.didichuxing.horoscope.runtime.interpreter.{CompatibleInterpreter, FlowInterpreter}
import com.didichuxing.horoscope.util.Logging
import com.google.common.cache.{Cache, CacheBuilder}
import com.typesafe.config.Config

import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.language.implicitConversions
import scala.util.{Failure, Try}

class FlowExecutorImpl(
  config: Config, actorSystem: ActorSystem, env: Environment
) extends FlowExecutor with Logging {
  import FlowExecutorImpl._
  import env._

  override def start(): Unit = {
    manager = actorSystem.actorOf(
      Props(classOf[Manager], this), actorName
    )
  }

  override def stop(): Unit = {
    manager ! PoisonPill
  }

  override def execute(event: FlowEvent): Future[FlowInstance] = {
    val instance = Promise[FlowInstance]()
    manager ! ExecuteEvent(event, instance)

    instance.future
  }

  private val contextCache: Cache[String, Map[String, TraceVariable]] = CacheBuilder.newBuilder()
    .maximumSize(Try(config.getLong("horoscope.flow-executor.context-cache.maximum-size")).getOrElse(100000))
    .concurrencyLevel(128)
    .expireAfterAccess(
      Try(config.getLong("horoscope.flow-executor.context-cache.expire-after-access-minutes")).getOrElse(20),
      TimeUnit.MINUTES
    ).build()

  private var manager: ActorRef = actorSystem.deadLetters

  class Manager extends Actor {
    override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(3) {
      case _: Exception => Restart
    }

    class TraceQueue(val traceId: String) {
      type PendingEvent = (FlowEvent, Promise[FlowInstance])
      type RunningEvent = (FlowEvent, Promise[FlowInstance], ActorRef)

      val pendingEvents: mutable.Queue[PendingEvent] = mutable.Queue()
      var runningEvent: Option[RunningEvent] = None

      def add(event: FlowEvent, promise: Promise[FlowInstance]): Unit = {
        pendingEvents.enqueue(event -> promise)
      }

      def finish(
        eventId: String, instance: Try[FlowInstance], traceContext: Map[String, TraceVariable]
      ): Unit = {
        if (runningEvent.exists(_._1.getEventId == eventId)) {
          val (_, promise, worker) = runningEvent.get

          promise.tryComplete(instance)
          contextCache.put(traceId, traceContext)

          context.unwatch(worker)
          context.stop(worker)
          runningEvent = None
        } else {
          logError(("msg", s"try to finish unknown event $eventId, while running one is $runningEvent"))
        }
      }

      def run(): Boolean = {
        while (pendingEvents.nonEmpty && runningEvent.isEmpty) {
          val (event, promise) = pendingEvents.dequeue()
          try {
            assert(shouldAccept(traceId), s"executor should not accept trace $traceId")
            val traceContext = contextCache.get(traceId, new Callable[Map[String, TraceVariable]] {
              def call(): Map[String, TraceVariable] = Map.empty
            })

            val flow = getFlowByName(event.getFlowName)
            val worker = if (flow.isCompatible) {
              context.actorOf(
                Props(classOf[CompatibleInterpreter], flow, event, env, manager, traceContext), event.getEventId
              )
            } else {
              context.actorOf(
                Props(classOf[FlowInterpreter], env, manager, event, traceContext, config), event.getEventId
              )
            }

            context.watchWith(
              worker,
              CommitEvent(event.getTraceId, event.getEventId, Failure(new Exception("actor dead")), Map.empty)
            )

            runningEvent = Some((event, promise, worker))
          } catch {
            case e: Throwable =>
              contextCache.invalidate(traceId)
              logError(("msg", s"execute event failed"), ("cause", e.getMessage))
              promise.failure(e)
          }
        }

        runningEvent.nonEmpty
      }
    }

    private val traces: mutable.Map[String, TraceQueue] = mutable.Map()

    override def receive: Receive = {
      case ExecuteEvent(event, promise) =>
        val traceId = event.getTraceId
        val trace = traces.getOrElseUpdate(traceId, new TraceQueue(traceId))
        trace.add(event, promise)
        if (!trace.run()) {
          traces.remove(trace.traceId)
        }

      case CommitEvent(traceId, eventId, instance, traceContext) =>
        val trace = traces.getOrElseUpdate(traceId, new TraceQueue(traceId))
        trace.finish(eventId, instance, traceContext)
        if (!trace.run()) {
          traces.remove(traceId)
        }
    }
  }

  // for unit test
  def actorName: String = ACTOR_NAME
}

object FlowExecutorImpl {
  case class ExecuteEvent(
    event: FlowEvent, promise: Promise[FlowInstance]
  )
  case class CommitEvent(
    traceId: String, eventId: String, instance: Try[FlowInstance], traceContext: Map[String, TraceVariable]
  )

  // fix unit test name conflict
  val ACTOR_NAME = "flow-executor"
  val ACTOR_PATH = s"/user/$ACTOR_NAME"
}
