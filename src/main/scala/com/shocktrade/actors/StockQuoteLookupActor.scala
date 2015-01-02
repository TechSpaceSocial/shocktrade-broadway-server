package com.shocktrade.actors

import akka.actor.{Actor, ActorRef}
import com.ldaniels528.broadway.server.etl.actors.FileReadingActor._
import com.ldaniels528.trifecta.io.avro.AvroConversion
import com.shocktrade.services.{YFStockQuoteService, YahooFinanceServices}

import scala.concurrent.ExecutionContext

/**
 * Stock Quote Lookup Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class StockQuoteLookupActor(target: ActorRef)(implicit ec: ExecutionContext) extends Actor {
  private val parameters = YFStockQuoteService.getParams(
    "symbol", "exchange", "lastTrade", "tradeDate", "tradeTime", "ask", "bid", "change", "changePct",
    "prevClose", "open", "close", "high", "low", "volume", "marketCap", "errorMessage")

  override def receive = {
    case OpeningFile(resource) =>
    case ClosingFile(resource) =>
    case TextLine(lineNo, line, tokens) =>
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
