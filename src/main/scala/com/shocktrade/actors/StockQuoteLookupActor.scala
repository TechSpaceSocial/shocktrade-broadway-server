package com.shocktrade.actors

import akka.actor.Actor
import com.ldaniels528.broadway.core.actors.FileReadingActor._
import com.ldaniels528.broadway.server.etl.BroadwayTopology.BWxActorRef
import com.ldaniels528.broadway.server.etl.BroadwayTopology.Implicits._
import com.ldaniels528.trifecta.io.avro.AvroConversion
import com.shocktrade.helpers.ResourceTracker
import com.shocktrade.services.{YFStockQuoteService, YahooFinanceServices}

import scala.concurrent.ExecutionContext

/**
 * Stock Quote Lookup Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class StockQuoteLookupActor(target: BWxActorRef)(implicit ec: ExecutionContext) extends Actor {
  private val parameters = YFStockQuoteService.getParams(
    "symbol", "exchange", "lastTrade", "tradeDate", "tradeTime", "ask", "bid", "change", "changePct",
    "prevClose", "open", "close", "high", "low", "volume", "marketCap", "errorMessage")

  override def receive = {
    case OpeningFile(resource) =>
      ResourceTracker.start(resource)

    case ClosingFile(resource) =>
      ResourceTracker.stop(resource)

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
