package com.shocktrade.datacenter.narratives.yahoo

import java.util.Properties

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor.{Publish, PublishAvro}
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor._
import com.ldaniels528.broadway.core.util.PropertiesHelper._
import com.ldaniels528.broadway.server.ServerConfig
import com.ldaniels528.trifecta.io.avro.AvroConversion
import com.mongodb.casbah.Imports.{DBObject => O, _}
import com.shocktrade.datacenter.narratives.yahoo.CsvQuotesYahooToKafkaNarrative.{QuoteLookupAndPublishActor, QuoteSymbolsActor, RequestQuotes}
import com.shocktrade.services.{YFStockQuoteService, YahooFinanceServices}
import org.joda.time.DateTime

/**
 * CSV Stock Quotes: Yahoo! Finance to Kafka Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class CsvQuotesYahooToKafkaNarrative(config: ServerConfig, id: String, props: Properties)
  extends BroadwayNarrative(config, id, props) {

  // extract the properties we need
  val kafkaTopic = props.getOrDie("kafka.topic")
  val mongoReplicas = props.getOrDie("mongo.replicas")
  val mongoDatabase = props.getOrDie("mongo.database")
  val mongoCollection = props.getOrDie("mongo.collection")
  val zkConnect = props.getOrDie("zookeeper.connect")

  // create a MongoDB actor for retrieving stock quotes
  lazy val mongoReader = prepareActor(MongoDBActor(parseServerList(mongoReplicas), mongoDatabase), parallelism = 1)

  // create a Kafka publishing actor for stock quotes
  // NOTE: the Kafka parallelism is equal to the number of brokers
  lazy val quotePublisher = prepareActor(new KafkaPublishingActor(zkConnect), parallelism = 6)

  // create a stock quote lookup actor
  lazy val quoteLookup = prepareActor(new QuoteLookupAndPublishActor(kafkaTopic, quotePublisher), parallelism = 1)

  // create a stock symbols requesting actor
  lazy val symbolsRequester = prepareActor(new QuoteSymbolsActor(mongoReader, mongoCollection, quoteLookup), parallelism = 1)

  onStart { resource =>
    symbolsRequester ! RequestQuotes
  }
}

/**
 * CSV Stock Quotes: Yahoo! Finance to Kafka Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object CsvQuotesYahooToKafkaNarrative {
  val parameters = YFStockQuoteService.getParams(
    "symbol", "exchange", "lastTrade", "tradeDate", "tradeTime", "change", "changePct", "prevClose", "open", "close",
    "high", "low", "high52Week", "low52Week", "volume", "marketCap", "errorMessage", "ask", "askSize", "bid", "bidSize")

  /**
   * Stock Quote Lookup and Publish Actor
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  class QuoteLookupAndPublishActor(topic: String, target: ActorRef) extends Actor with ActorLogging {

    import context.dispatcher

    override def receive = {
      case symbol: String => transmit(symbol)
      case symbols: Array[String] => transmit(symbols)
      case message =>
        log.error(s"Unhandled message $message")
        unhandled(message)
    }

    private def transmit(symbol: String) {
      YFStockQuoteService.getCSVData(Seq(symbol), parameters) foreach {
        _ foreach { line =>
          target ! Publish(topic, message = s"$symbol|$parameters|$line".getBytes("UTF8"))
        }
      }
    }

    private def transmit(symbols: Array[String]) {
      YFStockQuoteService.getCSVData(symbols, parameters) foreach { lines =>
        (symbols zip lines.toSeq) foreach { case (symbol, line) =>
          target ! Publish(topic, message = s"$symbol|$parameters|$line".getBytes("UTF8"))
        }
      }
    }

    private def transmitAsAvro(symbol: String) {
      YahooFinanceServices.getStockQuote(symbol, parameters) foreach { quote =>
        val builder = com.shocktrade.avro.CSVQuoteRecord.newBuilder()
        AvroConversion.copy(quote, builder)
        target ! PublishAvro(topic, record = builder.build())
      }
    }

    private def transmitAsAvro(symbols: Seq[String]) {
      YahooFinanceServices.getStockQuotes(symbols, parameters) foreach { quotes =>
        quotes foreach { quote =>
          val builder = com.shocktrade.avro.CSVQuoteRecord.newBuilder()
          AvroConversion.copy(quote, builder)
          target ! PublishAvro(topic, record = builder.build())
        }
      }
    }

  }

  /**
   * Stock Quote Symbol Retrieval Actor
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  class QuoteSymbolsActor(mongoReader: ActorRef, mongoCollection: String, quoteLookup: ActorRef) extends Actor with ActorLogging {
    override def receive = {
      // when a receive the request quotes message, I shall fire a find message to the MongoDB actor
      case RequestQuotes =>
        val _5_mins_ago = new DateTime().minusMinutes(5)
        mongoReader ! Find(
          name = mongoCollection,
          query = O("active" -> true, "yfDynUpdates" -> true) ++ $or("yfDynLastUpdated" $exists false, "yfDynLastUpdated" $lte _5_mins_ago),
          fields = O("symbol" -> 1)
        )

      // .. and I shall forward all responses to the quote lookup actor
      case MongoResult(doc) => doc.getAs[String]("symbol") foreach (quoteLookup ! _)

      case message => unhandled(message)
    }
  }

  case object RequestQuotes

}