package com.shocktrade.actors

import akka.actor.{ActorSystem, Props}
import akka.persistence.{PersistentActor, SnapshotOffer}
import org.slf4j.LoggerFactory

/**
 * Counter Example
 * @author lawrence.daniels@gmail.com
 */
object CounterExample {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  /**
   * Main entry point
   * @param args the given command line arguments
   */
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("example")
    val countingActor = system.actorOf(Props[CountingActor], "countingActor")
    """
      akka.persistence.journal.plugin = "kafka-journal"
    """

    countingActor ! 9
    countingActor ! 16
    countingActor ! "print"
    countingActor ! 25
    countingActor ! "print"
    countingActor ! "snap"
    Thread.sleep(1000)
    system.shutdown()
  }

  /**
   * Example Persistent Actor
   */
  class CountingActor extends PersistentActor {
    private var count = 0

    val persistenceId: String = "counter"

    override def receiveRecover = {
      case delta: Int => count += delta
      case SnapshotOffer(_, snapshot: Int) =>
        logger.info(s"Replaying snapshot $snapshot")
        count = snapshot
    }

    override def receiveCommand = {
      case delta: Int => count += delta
      case "snap" => saveSnapshot(count)
      case "print" => println(s"count = $count")
      case unknown => unhandled(unknown)
    }
  }

}
