package com.shocktrade.datacenter.actors

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.ldaniels528.trifecta.io.kafka.KafkaMicroConsumer._
import com.ldaniels528.trifecta.io.kafka.{Broker, KafkaMicroConsumer}
import com.ldaniels528.trifecta.io.zookeeper.ZKProxy
import com.ldaniels528.trifecta.util.ResourceHelper._
import com.shocktrade.datacenter.actors.ScanCoordinatingActor.Scan
import kafka.common.TopicAndPartition

/**
 * Consumer Reset Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ConsumerResetActor(target: ActorRef)(implicit zk: ZKProxy) extends Actor with ActorLogging {

  override def receive = {
    case scan@Scan(topic, groupId, brokers) =>
      resetGroup(topic, groupId, brokers)
      target ! scan

    case message =>
      unhandled(message)
  }

  private def resetGroup(topic: String, groupId: String, brokers: Seq[Broker]) {
    // reset the consumer IDs to the first offset
    val partitions = getTopicPartitions(topic)
    partitions foreach { partition =>
      new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use { subs =>
        subs.getFirstOffset foreach { offset =>
          log.info(s"Setting consumer $groupId for partition $topic:$partition to $offset...")
          subs.commitOffsets(groupId, offset, metadata = "resetting ID")
        }
      }
    }
  }
}
