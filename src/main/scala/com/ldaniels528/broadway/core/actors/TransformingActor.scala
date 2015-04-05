package com.ldaniels528.broadway.core.actors

import akka.actor.{Actor, ActorRef}

/**
 * Data Transforming Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class TransformingActor[T](recipient: ActorRef, transform: Any => Option[T]) extends Actor {
  override def receive = {
    case message => transform(message) foreach (recipient ! _)
  }
}
