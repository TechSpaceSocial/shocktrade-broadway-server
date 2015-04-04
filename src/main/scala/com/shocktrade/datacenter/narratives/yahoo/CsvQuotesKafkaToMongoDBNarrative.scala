package com.shocktrade.datacenter.narratives.yahoo

import java.util.{Date, Properties}

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.util.Timeout
import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.kafka.KafkaConsumingActor
import com.ldaniels528.broadway.core.actors.kafka.KafkaConsumingActor.{MessageReceived, StartConsuming}
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor._
import com.ldaniels528.broadway.core.util.PropertiesHelper._
import com.ldaniels528.broadway.server.ServerConfig
import com.ldaniels528.trifecta.util.OptionHelper._
import com.mongodb.casbah.Imports.{DBObject => O, _}
import com.shocktrade.datacenter.narratives.yahoo.CsvQuotesKafkaToMongoDBNarrative.CSVQuoteTransformActor
import com.shocktrade.services.YFStockQuoteService
import com.shocktrade.services.YFStockQuoteService.YFStockQuote

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
 * CSV Stock Quotes: Kafka to MongoDB Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class CsvQuotesKafkaToMongoDBNarrative(config: ServerConfig, id: String, props: Properties)
  extends BroadwayNarrative(config, id, props) {

  // extract the properties we need
  val kafkaTopic = props.getOrDie("kafka.topic")
  val mongoReplicas = props.getOrDie("mongo.replicas")
  val mongoDatabase = props.getOrDie("mongo.database")
  val mongoCollection = props.getOrDie("mongo.collection")
  val zkConnect = props.getOrDie("zookeeper.connect")

  // create a MongoDB actor for persisting stock quotes
  lazy val mongoWriter = prepareActor(MongoDBActor(parseServerList(mongoReplicas), mongoDatabase), parallelism = 5)

  // create a CSV to Stock Quote object transforming actor
  lazy val quoteParser = prepareActor(new CSVQuoteTransformActor(mongoCollection, mongoWriter), parallelism = 1)

  // create the Kafka message consumer
  lazy val kafkaConsumer = prepareActor(new KafkaConsumingActor(zkConnect), parallelism = 1)

  onStart { resource =>
    kafkaConsumer ! StartConsuming(kafkaTopic, quoteParser)
  }
}

/**
 * CSV Stock Quotes: Kafka to MongoDB Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object CsvQuotesKafkaToMongoDBNarrative {

  /**
   * Stock Quote Transform Actor
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  class CSVQuoteTransformActor(collectionName: String, target: ActorRef)(implicit ec: ExecutionContext) extends Actor with ActorLogging {
    override def receive = {
      case m: MessageReceived =>
        val startTime = System.currentTimeMillis()
        val line = new String(m.message, "UTF8")
        line.split("[|]").toList match {
          case symbol :: params :: csv :: Nil =>
            val quote = YFStockQuoteService.parseQuote(symbol, params, csv, startTime)
            persistQuote(quote)

          case _ =>
            log.error(s"Malformed data received: $line")
        }
      case message => unhandled(message)
    }

    private def persistQuote(record: YFStockQuote) = {
      import akka.pattern.ask

      val newSymbol = record.newSymbol
      val oldSymbol = Option(record.symbol) // record.oldSymbol

      // if a new symbol is being created, we need to make sure the base record
      // is created before any updates occur.
      // TODO instead setup an optional callback once the create/update operation is complete
      if (newSymbol.isDefined) {
        // create/update the record
        implicit val timeout: Timeout = 30.seconds
        (target ? createOrUpdate(record)).foreach { _ =>

          // if the symbol was changed  update the old record
          newSymbol.foreach { symbol =>
            target ! Upsert(collectionName, query = O("symbol" -> oldSymbol), doc = O("symbol" -> oldSymbol))
            target ! Upsert(collectionName, query = O("symbol" -> symbol), doc = $set("oldSymbol" -> oldSymbol))
          }
        }
      }

      // just fire-and-forget the create/update
      else {
        target ! createOrUpdate(record)
      }
    }

    private def createOrUpdate(q: YFStockQuote) = {
      val newSymbol = q.newSymbol
      val oldSymbol = Option(q.symbol) // q.oldSymbol
      val theSymbol = newSymbol ?? oldSymbol

      Upsert(
        collectionName,
        query = O("symbol" -> theSymbol),
        doc = $set(
          "exchange" -> q.exchange,
          "lastTrade" -> q.lastTrade,
          "tradeDate" -> q.tradeDate,
          "tradeDateTime" -> q.tradeDateTime,
          "ask" -> q.ask,
          "bid" -> q.bid,
          "prevClose" -> q.prevClose,
          "open" -> q.open,
          "close" -> q.close,
          "change" -> q.change,
          "changePct" -> q.changePct ?? computeChangePct(q.prevClose, q.lastTrade),
          "high" -> q.high,
          "low" -> q.low,
          "spread" -> computeSpread(q.high, q.low),
          "volume" -> q.volume,

          // classification fields
          "assetType" -> "Common Stock",
          "assetClass" -> "Equity",

          // administrative fields
          "yfDynRespTimeMsec" -> q.responseTimeMsec,
          "yfDynLastUpdated" -> new Date(),
          "lastUpdated" -> new Date()))
    }

    private def computeSpread(high: Option[Double], low: Option[Double]) = {
      for {
        hi <- high
        lo <- low
      } yield if (lo != 0.0d) 100d * (hi - lo) / lo else 0.0d
    }

    private def computeChangePct(prevClose: Option[Double], lastTrade: Option[Double]) = {
      for {
        prev <- prevClose
        last <- lastTrade
        diff = last - prev
      } yield if (diff != 0) 100d * (diff / prev) else 0.0d
    }

  }

}