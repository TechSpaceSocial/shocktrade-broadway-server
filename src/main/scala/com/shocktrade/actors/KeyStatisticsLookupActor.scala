package com.shocktrade.actors

import akka.actor.{Actor, ActorRef}
import com.ldaniels528.broadway.server.etl.actors.FileReadingActor._
import com.ldaniels528.trifecta.io.avro.AvroConversion
import com.shocktrade.services.YahooFinanceServices

import scala.concurrent.ExecutionContext

/**
 * Key Statistics Lookup Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KeyStatisticsLookupActor(target: ActorRef)(implicit ec: ExecutionContext) extends Actor {

  override def receive = {
    case OpeningFile(resource) =>
    case ClosingFile(resource) =>
    case TextLine(lineNo, line, tokens) =>
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

