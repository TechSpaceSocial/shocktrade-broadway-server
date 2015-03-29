package com.shocktrade.narratives

import akka.actor.{Actor, ActorRef}
import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.FileReadingActor
import com.ldaniels528.broadway.core.actors.FileReadingActor._
import com.ldaniels528.broadway.core.actors.kafka.avro.KafkaAvroPublishingActor
import com.ldaniels528.broadway.core.resources.ReadableResource
import com.ldaniels528.broadway.server.ServerConfig
import com.ldaniels528.trifecta.io.avro.AvroConversion
import com.shocktrade.helpers.ResourceTracker
import com.shocktrade.narratives.YFKeyStatsImportNarrative.KeyStatisticsLookupActor
import com.shocktrade.services.YahooFinanceServices

import scala.concurrent.ExecutionContext

/**
 * Yahoo! Finance Key Statistics Import Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class YFKeyStatsImportNarrative(config: ServerConfig) extends BroadwayNarrative(config, "Key Statistics Import")
with KafkaConstants {

  // create a file reader actor to read lines from the incoming resource
  lazy val fileReader = addActor(new FileReadingActor(config))

  // create a Kafka publishing actor for stock quotes
  lazy val keyStatsPublisher = addActor(new KafkaAvroPublishingActor(keyStatsTopic, brokers))

  // create a stock quote lookup actor
  lazy val keyStatsLookup = addActor(new KeyStatisticsLookupActor(keyStatsPublisher))

  onStart {
    case resource: ReadableResource =>
      // start the processing by submitting a request to the file reader actor
      fileReader ! CopyText(resource, keyStatsLookup, handler = Delimited("[\t]"))
    case _ =>
      throw new IllegalStateException(s"A ${classOf[ReadableResource].getName} was expected")
  }
}

/**
 * Yahoo! Finance Key Statistics Import Narrative Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object YFKeyStatsImportNarrative {

  /**
   * Key Statistics Lookup Actor
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  class KeyStatisticsLookupActor(target: ActorRef)(implicit ec: ExecutionContext) extends Actor {
    override def receive = {
      case OpeningFile(resource) =>
        ResourceTracker.start(resource)

      case ClosingFile(resource) =>
        ResourceTracker.stop(resource)

      case TextLine(resource, lineNo, line, tokens) =>
        tokens.headOption foreach { symbol =>
          YahooFinanceServices.getKeyStatistics(symbol) foreach { keyStatistics =>
            val builder = com.shocktrade.avro.KeyStatisticsRecord.newBuilder()
            AvroConversion.copy(keyStatistics, builder)
            target ! builder.build()
          }
        }

      case message =>
        unhandled(message)
    }
  }

}
