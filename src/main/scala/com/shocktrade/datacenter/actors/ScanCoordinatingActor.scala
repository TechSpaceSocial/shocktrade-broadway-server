package com.shocktrade.datacenter.actors

import akka.actor.{Actor, ActorRef}
import com.ldaniels528.trifecta.io.kafka.Broker
import com.ldaniels528.trifecta.io.kafka.KafkaMicroConsumer._
import com.ldaniels528.trifecta.io.zookeeper.ZKProxy
import com.shocktrade.datacenter.actors.ScanCoordinatingActor.Scan

/**
 * Scan Coordinating Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ScanCoordinatingActor(target: ActorRef)(implicit zk: ZKProxy) extends Actor {
  override def receive = {
    case groupId: String =>
      coordinate(groupId) foreach (target ! _)

    case message =>
      unhandled(message)
  }

  def coordinate(groupId: String): Seq[Scan] = {
    // lookup the Kafka brokers and available topics
    val brokers = getBrokerList map (b => Broker(b.host, b.port))
    val topics = getTopicList(brokers) map (_.topic)

    // pass each topic and group ID to a consumer scanner
    topics.map(Scan(_, groupId, brokers))
  }
}


/**
 * Scan Coordinating Actor Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object ScanCoordinatingActor {

  case class Scan(topic: String, groupId: String, brokers: Seq[Broker])

}
