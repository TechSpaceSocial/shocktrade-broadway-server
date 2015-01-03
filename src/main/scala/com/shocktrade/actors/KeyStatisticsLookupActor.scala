package com.shocktrade.actors

import akka.actor.{Actor, ActorRef}
import com.ldaniels528.broadway.core.resources._
import com.ldaniels528.broadway.server.etl.actors.FileReadingActor._
import com.ldaniels528.trifecta.io.avro.AvroConversion
import com.shocktrade.services.YahooFinanceServices
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

/**
 * Key Statistics Lookup Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KeyStatisticsLookupActor(target: ActorRef)(implicit ec: ExecutionContext) extends Actor {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val processing = TrieMap[ReadableResource, Long]()

  override def receive = {
    case OpeningFile(resource) =>
      processing(resource) = System.currentTimeMillis()

    case ClosingFile(resource) =>
      processing.get(resource) foreach { startTime =>
        logger.info(s"Resource $resource completed in ${System.currentTimeMillis() - startTime} msecs")
        processing -= resource
      }

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

