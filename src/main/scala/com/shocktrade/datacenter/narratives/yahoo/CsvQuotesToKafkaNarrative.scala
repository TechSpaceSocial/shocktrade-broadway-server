package com.shocktrade.datacenter.narratives.yahoo

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.kafka.avro.KafkaAvroPublishingActor
import com.ldaniels528.broadway.core.actors.kafka.avro.KafkaAvroPublishingActor.Publish
import com.ldaniels528.broadway.server.ServerConfig
import com.shocktrade.datacenter.narratives.KafkaConstants
import com.shocktrade.datacenter.narratives.yahoo.CsvQuotesToKafkaNarrative.{CSVQuoteLookupActor, topic}
import com.shocktrade.datacenter.resources.QuoteSymbolResource
import com.shocktrade.services.YFStockQuoteService

import scala.concurrent.ExecutionContext

/**
 * CSV Stock Quotes: Yahoo! Finance to Kafka Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class CsvQuotesToKafkaNarrative(config: ServerConfig) extends BroadwayNarrative(config, "CSV Quote Import") with KafkaConstants {

  // create a Kafka publishing actor for stock quotes
  // NOTE: the Kafka parallelism is equal to the number of brokers
  lazy val quotePublisher = addActor(new KafkaAvroPublishingActor(topic, brokers), parallelism = 6)

  // create a stock quote lookup actor
  lazy val quoteLookup = addActor(new CSVQuoteLookupActor(quotePublisher), parallelism = 1)

  onStart {
    case resource: QuoteSymbolResource =>
      resource.iterator.sliding(32, 32) foreach (quoteLookup ! _.toArray)
    case _ =>
      throw new IllegalStateException(s"A ${classOf[QuoteSymbolResource].getName} was expected")
  }
}

/**
 * CSV Stock Quotes: Yahoo! Finance to Kafka Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object CsvQuotesToKafkaNarrative {
  val topic = "quotes.yahoo.csv"
  val parameters = YFStockQuoteService.getParams(
    "symbol", "exchange", "lastTrade", "tradeDate", "tradeTime", "change", "changePct", "prevClose", "open", "close",
    "high", "low", "high52Week", "low52Week", "volume", "marketCap", "errorMessage", "ask", "askSize", "bid", "bidSize")

  /**
   * Stock Quote Lookup Actor
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  class CSVQuoteLookupActor(target: ActorRef)(implicit ec: ExecutionContext) extends Actor with ActorLogging {
    override def receive = {
      case symbols: Array[String] =>
        YFStockQuoteService.getCSVData(symbols, parameters) foreach { lines =>
          (symbols zip lines.toSeq) foreach { case (symbol, line) =>
            target ! Publish(message = s"$symbol|$parameters|$line".getBytes("UTF8"))
          }
        }
      case message => unhandled(message)
    }
  }

}