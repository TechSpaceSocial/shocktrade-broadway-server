package com.shocktrade.narratives

import java.util.Date

import akka.actor.Actor
import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.Actors.BWxActorRef
import com.ldaniels528.broadway.core.actors.Actors.Implicits._
import com.ldaniels528.broadway.core.actors.FileReadingActor
import com.ldaniels528.broadway.core.actors.FileReadingActor.CopyText
import com.ldaniels528.broadway.server.ServerConfig
import com.ldaniels528.broadway.thirdparty.mongodb.MongoDBActor
import com.ldaniels528.broadway.thirdparty.mongodb.MongoDBActor.Upsert
import com.ldaniels528.trifecta.util.OptionHelper._
import com.mongodb.casbah.Imports.{DBObject => Q, _}
import com.shocktrade.avro.CSVQuoteRecord
import com.shocktrade.narratives.YFStockQuoteToMongoDBNarrative.StockQuoteTransformingActor
import com.shocktrade.narratives.YFStockQuoteImportNarrative.StockQuoteLookupActor
import org.slf4j.LoggerFactory

/**
 * Yahoo! Finance Stock Quote Export to MongoDB Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class YFStockQuoteToMongoDBNarrative(config: ServerConfig) extends BroadwayNarrative(config, "Stock Quote Export")
with KafkaConstants with MongoDBConstants {
  // create a MongoDB actor for persisting stock quotes
  val mongoActor = addActor(new MongoDBActor(ShockTradeDB, MongoDBServers))

  // stock quote to MongoDB document transformer
  val transformer = addActor(new StockQuoteTransformingActor(mongoActor))

  // create a stock quote lookup actor
  val quoteLookup = addActor(new StockQuoteLookupActor(transformer))

  // create a file reader actor to read lines from the incoming resource
  // TODO create a Kafka consumer to use as the input device
  val fileReader = addActor(new FileReadingActor(config))

  onStart { resource =>
    // start the processing by submitting a request to the file reader actor
    fileReader ! CopyText(resource, quoteLookup)
  }
}

/**
 * Yahoo! Finance Stock Quote Export Narrative Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object YFStockQuoteToMongoDBNarrative extends MongoDBConstants {
  private[this] lazy val logger = LoggerFactory.getLogger(getClass)

  /**
   * Stock Quote Transforming Actor
   * @author lawrence.daniels@gmail.com
   */
  class StockQuoteTransformingActor(output: BWxActorRef) extends Actor {
    override def receive = {
      case record: CSVQuoteRecord =>
        persistDocument(record)

      case unknown =>
        logger.error(s"Received unrecognized command '$unknown'")
        unhandled(unknown)
    }

    private def persistDocument(record: CSVQuoteRecord) = {
      import record._
      val theSymbol = Option(getNewSymbol) ?? Option(getOldSymbol)

      output ! Upsert(
        collection = StockQuotes,
        query = Q("symbol" -> theSymbol),
        doc = $set(
          "lastTrade" -> getLastTrade,
          "tradeDate" -> getTradeDate,
          "tradeDateTime" -> getTradeDateTime,
          "ask" -> getAsk,
          "bid" -> getBid,
          "prevClose" -> getPrevClose,
          "open" -> getOpen,
          "close" -> getClose,
          "change" -> getChange,
          "changePct" -> getChangePct,
          "high" -> getHigh,
          "low" -> getLow,
          "spread" -> computeSpread(Option(getHigh), Option(getLow)),
          "volume" -> getVolume,

          // classification fields
          "assetType" -> "Common Stock",
          "assetClass" -> "Equity",

          // administrative fields
          "yfDynRespTimeMsec" -> getResponseTimeMsec,
          "yfDynLastUpdated" -> new Date(),
          "lastUpdated" -> new Date()))

      // if the symbol was changed  update the old record
      if (Option(getNewSymbol).isDefined) {
        output ! Upsert(StockQuotes, query = Q("symbol" -> getOldSymbol), doc = Q("symbol" -> getOldSymbol))
        output ! Upsert(StockQuotes, query = Q("symbol" -> getNewSymbol), doc = $set("oldSymbol" -> getOldSymbol))
      }
    }

    private def computeSpread(high: Option[Double], low: Option[Double]): Option[Double] = {
      for {
        hi <- high
        lo <- low
      } yield if (lo != 0.0d) 100d * (hi - lo) / lo else 0.0d
    }
  }

}