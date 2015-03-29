package com.shocktrade.narratives

import akka.actor.{Actor, ActorRef}
import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.FileReadingActor._
import com.ldaniels528.broadway.core.actors.kafka.avro._
import com.ldaniels528.broadway.core.resources.IterableResource
import com.ldaniels528.broadway.server.ServerConfig
import com.ldaniels528.trifecta.io.avro.AvroConversion
import com.shocktrade.helpers.ResourceTracker
import com.shocktrade.narratives.YFStockQuoteImportNarrative.StockQuoteLookupActor
import com.shocktrade.services.{YFStockQuoteService, YahooFinanceServices}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

/**
 * Yahoo! Finance Stock Quote Import Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class YFStockQuoteImportNarrative(config: ServerConfig) extends BroadwayNarrative(config, "Stock Quote Import")
with KafkaConstants {
  lazy val logger = LoggerFactory.getLogger(getClass)

  // create a Kafka publishing actor for stock quotes
  lazy val quotePublisher = addActor(new KafkaAvroPublishingActor(quotesTopic, brokers))

  // create a stock quote lookup actor
  lazy val quoteLookup = addActor(new StockQuoteLookupActor(quotePublisher))

  onStart {
    case resource: IterableResource[String] =>
      resource.iterator foreach (quoteLookup ! _)
    case _ =>
      throw new IllegalStateException(s"A ${classOf[IterableResource[_]].getName} was expected")
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
  class StockQuoteLookupActor(target: ActorRef)(implicit ec: ExecutionContext) extends Actor {
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