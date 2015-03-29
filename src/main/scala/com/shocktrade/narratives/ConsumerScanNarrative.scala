package com.shocktrade.narratives

import akka.actor.{Actor, ActorRef}
import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.server.ServerConfig
import com.ldaniels528.trifecta.io.kafka.KafkaMicroConsumer.{MessageData, _}
import com.ldaniels528.trifecta.io.kafka.{Broker, KafkaMicroConsumer}
import com.ldaniels528.trifecta.io.zookeeper.ZKProxy
import com.ldaniels528.trifecta.util.ResourceHelper._
import com.shocktrade.narratives.ConsumerScanNarrative._
import kafka.common.TopicAndPartition
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

/**
 * Consumer Scan Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ConsumerScanNarrative(config: ServerConfig) extends BroadwayNarrative(config, "Consumer Scan") with KafkaConstants {
  implicit val zk = ZKProxy(zkHost)

  // create the consumer group scanning actor
  lazy val scanActor = addActor(new ConsumerScanningActor())

  // create the consumer group reset actor
  lazy val resetActor = addActor(new ConsumerResetActor(scanActor))

  // create the partition coordinating actor
  lazy val coordinator = addActor(new ScanCoordinatingActor(resetActor))

  onStart { resource =>
    // start the processing by submitting a request to the file reader actor
    coordinator ! "dev"
  }
}

/**
 * Consumer Scan Narrative Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object ConsumerScanNarrative {
  private[this] val logger = LoggerFactory.getLogger(getClass)

  /**
   * Scan Coordinating Actor
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  class ScanCoordinatingActor(target: ActorRef)(implicit ec: ExecutionContext, zk: ZKProxy) extends Actor {

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
   * Consumer Reset Actor
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  class ConsumerResetActor(target: ActorRef)(implicit zk: ZKProxy) extends Actor {

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
            logger.info(s"Setting consumer $groupId for partition $topic:$partition to $offset...")
            subs.commitOffsets(groupId, offset, metadata = "resetting ID")
          }
        }
      }
    }
  }

  /**
   * Consumer Scanning Actor
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  class ConsumerScanningActor()(implicit ec: ExecutionContext, zk: ZKProxy) extends Actor {

    override def receive = {
      case Scan(topic, groupId, brokers) =>
        // traverse the topic
        var lastUpdate = System.currentTimeMillis()
        var total = 0L
        var lastTotal = 0L

        logger.info(s"Traversing topic $topic using consumer ID '$groupId'...")
        observe(topic, groupId, brokers) { message =>
          total += 1

          val deltaTime = (System.currentTimeMillis() - lastUpdate).toDouble / 1000d
          if (deltaTime >= 5d) {
            val deltaCount = total - lastTotal
            val mps = deltaCount.toDouble / deltaTime
            logger.info(f"messages processed = $deltaCount, messages/sec = $mps%.1f, total messages = $total")
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

  case class Scan(topic: String, groupId: String, brokers: Seq[Broker])

}