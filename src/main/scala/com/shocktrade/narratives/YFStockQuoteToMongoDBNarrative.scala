package com.shocktrade.narratives

import java.util.Date

import akka.actor.{Actor, ActorRef}
import akka.util.Timeout
import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.FileReadingActor
import com.ldaniels528.broadway.core.actors.FileReadingActor.CopyText
import com.ldaniels528.broadway.core.resources.ReadableResource
import com.ldaniels528.broadway.server.ServerConfig
import com.ldaniels528.broadway.thirdparty.mongodb.MongoDBActor
import com.ldaniels528.broadway.thirdparty.mongodb.MongoDBActor.{Upsert, parseServerList}
import com.ldaniels528.trifecta.util.OptionHelper._
import com.mongodb.casbah.Imports.{DBObject => Q, _}
import com.shocktrade.avro.CSVQuoteRecord
import com.shocktrade.narratives.YFStockQuoteImportNarrative.StockQuoteLookupActor
import com.shocktrade.narratives.YFStockQuoteToMongoDBNarrative.StockQuoteTransformingActor
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * Yahoo! Finance Stock Quote Export to MongoDB Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class YFStockQuoteToMongoDBNarrative(config: ServerConfig) extends BroadwayNarrative(config, "Stock Quote Export")
with KafkaConstants with MongoDBConstants {
  // create a MongoDB actor for persisting stock quotes
  lazy val mongoActor = addActor(MongoDBActor(parseServerList(MongoDBServers), ShockTradeDB))

  // stock quote to MongoDB document transformer
  lazy val transformer = addActor(new StockQuoteTransformingActor(mongoActor))

  // create a stock quote lookup actor
  lazy val quoteLookup = addActor(new StockQuoteLookupActor(transformer))

  // create a file reader actor to read lines from the incoming resource
  // TODO create a Kafka consumer to use as the input device
  lazy val fileReader = addActor(new FileReadingActor(config))

  onStart {
    case resource: ReadableResource =>
      // start the processing by submitting a request to the file reader actor
      fileReader ! CopyText(resource, quoteLookup)
    case _ =>
      throw new IllegalStateException(s"A ${classOf[ReadableResource].getName} was expected")
  }
}

/**
 * Yahoo! Finance Stock Quote Export Narrative Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object YFStockQuoteToMongoDBNarrative extends MongoDBConstants {
  private[this] lazy val logger = LoggerFactory.getLogger(getClass)
  private implicit val timeout = new Timeout(30.seconds)

  /**
   * Stock Quote Transforming Actor
   * @author lawrence.daniels@gmail.com
   */
  class StockQuoteTransformingActor(recipient: ActorRef) extends Actor {
    override def receive = {
      case record: CSVQuoteRecord =>
        persistDocument(record)

      case unknown =>
        logger.error(s"Received unrecognized command '$unknown'")
        unhandled(unknown)
    }

    private def persistDocument(record: CSVQuoteRecord) = {
      val newSymbol = Option(record.getNewSymbol)
      val oldSymbol = Option(record.getOldSymbol)
      val theSymbol = newSymbol ?? oldSymbol

      recipient ! createUpdate(record)

      // if the symbol was changed  update the old record
      newSymbol.foreach { symbol =>
        recipient ! Upsert(StockQuotes, query = Q("symbol" -> oldSymbol), doc = Q("symbol" -> oldSymbol))
        recipient ! Upsert(StockQuotes, query = Q("symbol" -> symbol), doc = $set("oldSymbol" -> oldSymbol))
      }
    }

    private def createUpdate(record: CSVQuoteRecord) = {
      val newSymbol = Option(record.getNewSymbol)
      val oldSymbol = Option(record.getOldSymbol)
      val theSymbol = newSymbol ?? oldSymbol

      Upsert(
        StockQuotes,
        query = Q("symbol" -> theSymbol),
        doc = $set(
          "lastTrade" -> record.getLastTrade,
          "tradeDate" -> record.getTradeDate,
          "tradeDateTime" -> record.getTradeDateTime,
          "ask" -> record.getAsk,
          "bid" -> record.getBid,
          "prevClose" -> record.getPrevClose,
          "open" -> record.getOpen,
          "close" -> record.getClose,
          "change" -> record.getChange,
          "changePct" -> record.getChangePct,
          "high" -> record.getHigh,
          "low" -> record.getLow,
          "spread" -> computeSpread(Option(record.getHigh: Double), Option(record.getLow: Double)),
          "volume" -> record.getVolume,

          // classification fields
          "assetType" -> "Common Stock",
          "assetClass" -> "Equity",

          // administrative fields
          "yfDynRespTimeMsec" -> record.getResponseTimeMsec,
          "yfDynLastUpdated" -> new Date(),
          "lastUpdated" -> new Date()))
    }

    private def computeSpread(high: Option[Double], low: Option[Double]): Option[Double] = {
      for {
        hi <- high
        lo <- low
      } yield if (lo != 0.0d) 100d * (hi - lo) / lo else 0.0d
    }
  }

}