package com.shocktrade.narratives

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.kafka.avro.KafkaAvroPublishingActor.PublishAvro
import com.ldaniels528.broadway.core.actors.kafka.avro._
import com.ldaniels528.broadway.core.resources.IterableResource
import com.ldaniels528.broadway.server.ServerConfig
import com.ldaniels528.trifecta.io.avro.AvroConversion
import com.shocktrade.narratives.YFStockQuoteImportNarrative.StockQuoteLookupActor
import com.shocktrade.services.{YFStockQuoteService, YahooFinanceServices}

import scala.concurrent.ExecutionContext

/**
 * Yahoo! Finance Stock Quote Import Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class YFStockQuoteImportNarrative(config: ServerConfig) extends BroadwayNarrative(config, "Stock Quote Import") with KafkaConstants {

  // create a Kafka publishing actor for stock quotes
  // NOTE: the Kafka parallelism is equal to the number of brokers
  lazy val quotePublisher = addActor(new KafkaAvroPublishingActor(quotesTopic, brokers), parallelism = brokers.count(_ == ',') + 1)

  // create a stock quote lookup actor
  lazy val quoteLookup = addActor(new StockQuoteLookupActor(quotePublisher), parallelism = 5)

  onStart {
    case resource: IterableResource[String] =>
      resource.iterator.sliding(32, 32) foreach { symbols =>
        quoteLookup ! symbols.toArray
      }
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
  class StockQuoteLookupActor(target: ActorRef)(implicit ec: ExecutionContext) extends Actor with ActorLogging {
    private val parameters = YFStockQuoteService.getParams(
      "symbol", "exchange", "lastTrade", "tradeDate", "tradeTime", "change", "changePct",
      "prevClose", "open", "close", "high", "low", "volume", "marketCap", "errorMessage",
      "ask", "bid")

    override def receive = {
      case symbol: String => transmit(symbol)
      case symbols: Array[String] => transmit(symbols)
      case message =>
        unhandled(message)
    }

    private def transmit(symbol: String): Unit = {
      YahooFinanceServices.getStockQuote(symbol, parameters) foreach { quote =>
        val builder = com.shocktrade.avro.CSVQuoteRecord.newBuilder()
        AvroConversion.copy(quote, builder)
        target ! PublishAvro(builder.build())
      }
    }

    private def transmit(symbols: Seq[String]): Unit = {
      YahooFinanceServices.getStockQuotes(symbols, parameters) foreach { quotes =>
        quotes foreach { quote =>
          val builder = com.shocktrade.avro.CSVQuoteRecord.newBuilder()
          AvroConversion.copy(quote, builder)
          target ! PublishAvro(builder.build())
        }
      }
    }

  }

}