package com.shocktrade.actors

import akka.actor.{Actor, ActorRef}
import com.ldaniels528.broadway.core.resources._
import com.ldaniels528.broadway.server.etl.actors.FileReadingActor._
import com.ldaniels528.trifecta.io.avro.AvroConversion
import com.shocktrade.services.{YFStockQuoteService, YahooFinanceServices}
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

/**
 * Stock Quote Lookup Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class StockQuoteLookupActor(target: ActorRef)(implicit ec: ExecutionContext) extends Actor {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val processing = TrieMap[ReadableResource, Long]()
  private val parameters = YFStockQuoteService.getParams(
    "symbol", "exchange", "lastTrade", "tradeDate", "tradeTime", "ask", "bid", "change", "changePct",
    "prevClose", "open", "close", "high", "low", "volume", "marketCap", "errorMessage")

  override def receive = {
    case OpeningFile(resource) =>
      processing(resource) = System.currentTimeMillis()

    case ClosingFile(resource) =>
      processing.get(resource) foreach { startTime =>
        logger.info(s"Resource $resource completed in ${System.currentTimeMillis() - startTime} msecs")
        processing -= resource
      }

    case TextLine(resource, lineNo, line, tokens) =>
      tokens.headOption foreach { symbol =>
        YahooFinanceServices.getStockQuote(symbol, parameters) foreach { quote =>
          val builder = com.shocktrade.avro.CSVQuoteRecord.newBuilder()
          AvroConversion.copy(quote, builder)
          target ! builder.build()
        }
      }

    case message =>
      unhandled(message)
  }
}
