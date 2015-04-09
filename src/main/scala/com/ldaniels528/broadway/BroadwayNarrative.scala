package com.ldaniels528.broadway

import java.util.Properties

import akka.actor.Actor
import com.ldaniels528.broadway.core.resources._
import com.ldaniels528.broadway.server.ServerConfig

import scala.concurrent.ExecutionContext
import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * This class describes a narrative; or flow for a given data process
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class BroadwayNarrative(config: ServerConfig, name: String, props: Properties) {
  protected implicit val executionContext = config.system.dispatcher
  private var setup: Option[Option[Resource] => Unit] = None
  private var tearDown: Option[() => Unit] = None

  /**
   * Prepares a new actor for execution within the narrative
   * @param actor the given [[Actor]]
   * @param parallelism the number of actors to create
   * @tparam T the actor type
   * @return an actor reference
   */
  def prepareActor[T <: Actor : ClassTag](actor: => T, parallelism: Int = 1) = config.prepareActor(actor, parallelism)

  /**
   * Setups the actions that will occur upon start of the narrative
   * @param block the executable block
   */
  def onStart(block: Option[Resource] => Unit)(implicit ec: ExecutionContext) = {
    setup = Option(block)
  }

  /**
   * Tears down the actions that were setup at the start of the narrative
   * @param block the executable block
   */
  def onStop(block: () => Unit)(implicit ec: ExecutionContext) = {
    tearDown = Option(block)
  }

  /**
   * Starts executing the narrative
   */
  def start(resource: Option[Resource]): Unit = setup.foreach(_(resource))

  /**
   * Stops the execution of the narrative
   */
  def stop(): Unit = tearDown.foreach(_())

}
