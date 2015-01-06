package com.shocktrade.narratives

import akka.actor.Actor
import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.Actors._
import com.ldaniels528.broadway.core.actors.FileReadingActor._
import com.ldaniels528.broadway.core.actors._
import com.ldaniels528.broadway.core.actors.kafka.avro._
import com.ldaniels528.broadway.server.ServerConfig
import com.ldaniels528.trifecta.io.avro.AvroConversion
import com.shocktrade.helpers.ResourceTracker
import com.shocktrade.narratives.YFStockQuoteImportNarrative.StockQuoteLookupActor
import com.shocktrade.services.{YFStockQuoteService, YahooFinanceServices}

import scala.concurrent.ExecutionContext

/**
 * Yahoo! Finance Stock Quote Import Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class YFStockQuoteImportNarrative(config: ServerConfig) extends BroadwayNarrative(config, "Stock Quote Import")
with KafkaConstants {
  // create a file reader actor to read lines from the incoming resource
  val fileReader = addActor(new FileReadingActor(config))

  // create a Kafka publishing actor for stock quotes
  val quotePublisher = addActor(new KafkaAvroPublishingActor(quotesTopic, brokers))

  // create a stock quote lookup actor
  val quoteLookup = addActor(new StockQuoteLookupActor(quotePublisher))

  onStart { resource =>
    // start the processing by submitting a request to the file reader actor
    fileReader ! CopyText(resource, quoteLookup, handler = Delimited("[\t]"))
  }
}

/**
 * Yahoo! Finance Stock Quote Import Narrative Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object YFStockQuoteImportNarrative {

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

}