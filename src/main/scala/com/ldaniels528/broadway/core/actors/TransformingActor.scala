package com.ldaniels528.broadway.core.actors

import com.ldaniels528.broadway.core.util.Counter

/**
 * Data Transforming Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class TransformingActor(transform: Any => Boolean, counter: Option[Counter] = None) extends BroadwayActor {
  override def receive = {
    case message =>
      if (transform(message)) counter.foreach(_ += 1)
      else {
        counter.foreach(_ -= 1)
        unhandled(message)
      }
  }
}
