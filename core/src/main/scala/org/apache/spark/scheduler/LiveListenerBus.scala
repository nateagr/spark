/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

import java.util.{List => JList}
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.DynamicVariable

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._

/**
 * Asynchronously passes SparkListenerEvents to registered SparkListeners.
 *
 * Until `start()` is called, all posted events are only buffered. Only after this listener bus
 * has started will events be actually propagated to all attached listeners. This listener bus
 * is stopped when `stop()` is called, and it will drop further events after stopping.
 */
private[spark] class LiveListenerBus(conf: SparkConf) {

  import LiveListenerBus._

  private var sparkContext: SparkContext = _

  // Indicate if `start()` is called
  private val started = new AtomicBoolean(false)
  // Indicate if `stop()` is called
  private val stopped = new AtomicBoolean(false)

  /** A counter for dropped events. It will be reset every time we log it. */
  private val droppedEventsCounter = new AtomicLong(0L)

  /** When `droppedEventsCounter` was logged last time in milliseconds. */
  @volatile private var lastReportTimestamp = 0L

  private val queues = new CopyOnWriteArrayList[AsyncEventQueue]()

  // Visible for testing.
  @volatile private[scheduler] var queuedEvents = new mutable.ListBuffer[SparkListenerEvent]()

  /** Add a listener to queue shared by all non-internal listeners. */
  def addToSharedQueue(listener: SparkListenerInterface): Unit = {
    addToQueue(listener, SHARED_QUEUE)
  }

  /** Add a listener to the executor management queue. */
  def addToManagementQueue(listener: SparkListenerInterface): Unit = {
    addToQueue(listener, EXECUTOR_MANAGEMENT_QUEUE)
  }

  /** Add a listener to the application status queue. */
  def addToStatusQueue(listener: SparkListenerInterface): Unit = {
    addToQueue(listener, APP_STATUS_QUEUE)
  }

  /** Add a listener to the event log queue. */
  def addToEventLogQueue(listener: SparkListenerInterface): Unit = {
    addToQueue(listener, EVENT_LOG_QUEUE)
  }

  /**
   * Add a listener to a specific queue, creating a new queue if needed. Queues are independent
   * of each other (each one uses a separate thread for delivering events), allowing slower
   * listeners to be somewhat isolated from others.
   */
  private def addToQueue(listener: SparkListenerInterface, queue: String): Unit = synchronized {
    if (stopped.get()) {
      throw new IllegalStateException("LiveListenerBus is stopped.")
    }

    queues.asScala.find(_.name == queue) match {
      case Some(queue) =>
        queue.addListener(listener)

      case None =>
        val newQueue = new AsyncEventQueue(queue, conf)
        newQueue.addListener(listener)
        if (started.get()) {
          newQueue.start(sparkContext)
        }
        queues.add(newQueue)
    }
  }

  def removeListener(listener: SparkListenerInterface): Unit = synchronized {
    // Remove listener from all queues it was added to, and stop queues that have become empty.
    queues.asScala
      .filter { queue =>
        queue.removeListener(listener)
        queue.listeners.isEmpty()
      }
      .foreach { toRemove =>
        if (started.get() && !stopped.get()) {
          toRemove.stop()
        }
        queues.remove(toRemove)
      }
  }

  /** Post an event to all queues. */
  def post(event: SparkListenerEvent): Unit = {
    if (stopped.get()) {
      return
    }

    // If the event buffer is null, it means the bus has been started and we can avoid
    // synchronization and post events directly to the queues. This should be the most
    // common case during the life of the bus.
    if (queuedEvents == null) {
      postToQueues(event)
      return
    }

    // Otherwise, need to synchronize to check whether the bus is started, to make sure the thread
    // calling start() picks up the new event.
    synchronized {
      if (!started.get()) {
        queuedEvents += event
        return
      }
    }

    // If the bus was already started when the check above was made, just post directly to the
    // queues.
    postToQueues(event)
  }

  private def postToQueues(event: SparkListenerEvent): Unit = {
    val it = queues.iterator()
    while (it.hasNext()) {
      it.next().post(event)
    }
  }

  /**
   * Start sending events to attached listeners.
   *
   * This first sends out all buffered events posted before this listener bus has started, then
   * listens for any additional events asynchronously while the listener bus is still running.
   * This should only be called once.
   *
   */
  def start(sc: SparkContext): Unit = synchronized {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException("LiveListenerBus already started.")
    }

    this.sparkContext = sc
    queues.asScala.foreach { q =>
      q.start(sc)
      queuedEvents.foreach(q.post)
    }
    queuedEvents = null
  }

  /**
   * For testing only. Wait until there are no more events in the queue, or until the specified
   * time has elapsed. Throw `TimeoutException` if the specified time elapsed before the queue
   * emptied.
   * Exposed for testing.
   */
  @throws(classOf[TimeoutException])
  def waitUntilEmpty(timeoutMillis: Long): Unit = {
    val deadline = System.currentTimeMillis + timeoutMillis
    queues.asScala.foreach { queue =>
      if (!queue.waitUntilEmpty(deadline)) {
        throw new TimeoutException(s"The event queue is not empty after $timeoutMillis ms.")
      }
    }
  }

  /**
   * Stop the listener bus. It will wait until the queued events have been processed, but drop the
   * new events after stopping.
   */
  def stop(): Unit = {
    if (!started.get()) {
      throw new IllegalStateException(s"Attempted to stop bus that has not yet started!")
    }

    if (!stopped.compareAndSet(false, true)) {
      return
    }

    synchronized {
      queues.asScala.foreach(_.stop())
      queues.clear()
    }
  }

  // For testing only.
  private[spark] def findListenersByClass[T <: SparkListenerInterface : ClassTag](): Seq[T] = {
    queues.asScala.flatMap { queue => queue.findListenersByClass[T]() }
  }

  // For testing only.
  private[spark] def listeners: JList[SparkListenerInterface] = {
    queues.asScala.flatMap(_.listeners.asScala).asJava
  }

  // For testing only.
  private[scheduler] def activeQueues(): Set[String] = {
    queues.asScala.map(_.name).toSet
  }

}

private[spark] object LiveListenerBus {
  // Allows for Context to check whether stop() call is made within listener thread
  val withinListenerThread: DynamicVariable[Boolean] = new DynamicVariable[Boolean](false)

  private[scheduler] val SHARED_QUEUE = "shared"

  private[scheduler] val APP_STATUS_QUEUE = "appStatus"

  private[scheduler] val EXECUTOR_MANAGEMENT_QUEUE = "executorManagement"

  private[scheduler] val EVENT_LOG_QUEUE = "eventLog"
}
