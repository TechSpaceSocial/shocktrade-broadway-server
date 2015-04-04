package com.shocktrade.datacenter.actors

import akka.actor.{Actor, ActorLogging}
import com.ldaniels528.trifecta.io.kafka.KafkaMicroConsumer._
import com.ldaniels528.trifecta.io.kafka.{Broker, KafkaMicroConsumer}
import com.ldaniels528.trifecta.io.zookeeper.ZKProxy
import com.ldaniels528.trifecta.util.ResourceHelper._
import com.shocktrade.datacenter.actors.ScanCoordinatingActor.Scan
import kafka.common.TopicAndPartition

import scala.concurrent.{ExecutionContext, Future}

/**
 * Consumer Scanning Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ConsumerScanningActor()(implicit zk: ZKProxy) extends Actor with ActorLogging {

  import context.dispatcher

  override def receive = {
    case Scan(topic, groupId, brokers) =>
      // traverse the topic
      var lastUpdate = System.currentTimeMillis()
      var total = 0L
      var lastTotal = 0L

      log.info(s"Traversing topic $topic using consumer ID '$groupId'...")
      observe(topic, groupId, brokers) { message =>
        total += 1

        val deltaTime = (System.currentTimeMillis() - lastUpdate).toDouble / 1000d
        if (deltaTime >= 5d) {
          val deltaCount = total - lastTotal
          val mps = deltaCount.toDouble / deltaTime
          log.info(f"messages processed = $deltaCount, messages/sec = $mps%.1f, total messages = $total")
          lastTotal = total
          lastUpdate = System.currentTimeMillis()
        }
      }
      ()
  }

  /**
   * Returns the promise of the option of a message based on the given search criteria
   * @param topic the given topic name
   * @param groupId the given consumer group ID
   * @param brokers the given replica brokers
   * @param observer the given callback function
   * @return the promise of the option of a message based on the given search criteria
   */
  private def observe(topic: String, groupId: String, brokers: Seq[Broker])(observer: MessageData => Unit)(implicit ec: ExecutionContext, zk: ZKProxy): Future[Seq[Unit]] = {
    Future.sequence(getTopicPartitions(topic) map { partition =>
      Future {
        var lastUpdate = System.currentTimeMillis()
        new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use { subs =>
          var offset: Option[Long] = subs.getFirstOffset
          val lastOffset: Option[Long] = subs.getLastOffset
          def eof: Boolean = offset.exists(o => lastOffset.exists(o > _))
          while (!eof) {
            // allow the observer to process the messages
            for (ofs <- offset; msg <- subs.fetch(ofs)(fetchSize = 65535).headOption) observer(msg)

            // commit the offset once per second
            if (System.currentTimeMillis() - lastUpdate >= 1000) {
              offset.foreach(subs.commitOffsets(groupId, _, ""))
              lastUpdate = System.currentTimeMillis()
            }
            offset = offset map (_ + 1)
          }

          // commit the final offset for each partition
          lastOffset.foreach(subs.commitOffsets(groupId, _, ""))
        }
      }
    })
  }
}
