package com.shocktrade.datacenter.actors.yahoo

import akka.actor.{Actor, ActorRef}
import com.ldaniels528.broadway.core.actors.file.FileReadingActor
import FileReadingActor.{TextLine, ClosingFile, OpeningFile}
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor.PublishAvro
import com.ldaniels528.trifecta.io.avro.AvroConversion
import com.shocktrade.datacenter.helpers.ResourceTracker
import com.shocktrade.services.YahooFinanceServices

import scala.concurrent.ExecutionContext

/**
 * Key Statistics Lookup Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KeyStatisticsLookupActor(topic: String, target: ActorRef)(implicit ec: ExecutionContext) extends Actor {
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
          target ! PublishAvro(topic, builder.build())
        }
      }

    case message =>
      unhandled(message)
  }
}