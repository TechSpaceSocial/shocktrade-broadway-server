package com.ldaniels528.broadway.core.actors

import akka.actor.Actor

/**
 * Data Transforming Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class TransformingActor(transform: Any => Unit) extends Actor {
  override def receive = {
    case message => transform(message)
  }
}
