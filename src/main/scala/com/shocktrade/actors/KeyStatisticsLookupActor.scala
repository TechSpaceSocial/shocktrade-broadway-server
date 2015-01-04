package com.shocktrade.actors

import akka.actor.Actor
import com.ldaniels528.broadway.core.actors.FileReadingActor.{TextLine, ClosingFile, OpeningFile}
import com.ldaniels528.broadway.server.etl.BroadwayTopology.BWxActorRef
import com.ldaniels528.broadway.server.etl.BroadwayTopology.Implicits._
import com.ldaniels528.trifecta.io.avro.AvroConversion
import com.shocktrade.helpers.ResourceTracker
import com.shocktrade.services.YahooFinanceServices

import scala.concurrent.ExecutionContext

/**
 * Key Statistics Lookup Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KeyStatisticsLookupActor(target: BWxActorRef)(implicit ec: ExecutionContext) extends Actor {
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
