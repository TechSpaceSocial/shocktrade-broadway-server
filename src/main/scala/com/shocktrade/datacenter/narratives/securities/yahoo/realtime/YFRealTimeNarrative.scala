package com.shocktrade.datacenter.narratives.securities.yahoo.realtime

import java.lang.{Double => JDouble, Long => JLong}
import java.util.{Date, Properties}

import akka.actor.ActorRef
import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.TransformingActor
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor.PublishAvro
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor.{Upsert, _}
import com.ldaniels528.broadway.core.util.Counter
import com.ldaniels528.broadway.server.ServerConfig
import com.ldaniels528.commons.helpers.OptionHelper._
import com.ldaniels528.commons.helpers.PropertiesHelper._
import com.mongodb.casbah.Imports.{DBObject => O, _}
import com.shocktrade.avro.YahooRealTimeQuoteRecord
import com.shocktrade.datacenter.narratives.securities.StockQuoteSupport
import com.shocktrade.services.YFRealtimeStockQuoteService
import com.shocktrade.services.YFRealtimeStockQuoteService.YFRealtimeQuote
import org.apache.avro.generic.GenericRecord
import org.joda.time.DateTime

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
 * Yahoo! Finance Real-time Quotes Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class YFRealTimeNarrative(config: ServerConfig, id: String, props: Properties)
  extends BroadwayNarrative(config, id, props)
  with StockQuoteSupport {

  // extract the properties we need
  val kafkaTopic = props.getOrDie("kafka.topic")
  val topicParallelism = props.getOrDie("kafka.topic.parallelism").toInt
  val mongoReplicas = props.getOrDie("mongo.replicas")
  val mongoDatabase = props.getOrDie("mongo.database")
  val mongoCollection = props.getOrDie("mongo.collection")
  val zkConnect = props.getOrDie("zookeeper.connect")

  // create a MongoDB actor for retrieving stock quotes
  lazy val mongoActor = prepareActor(MongoDBActor(parseServerList(mongoReplicas), mongoDatabase), parallelism = 30)

  // create a Kafka publishing actor
  lazy val kafkaPublisher = prepareActor(new KafkaPublishingActor(zkConnect), parallelism = 30)

  // create a counter for statistics
  val counter = new Counter(1.minute)((successes, failures, rps) =>
    log.info(f"Yahoo -> $kafkaTopic: $successes records, $failures failures ($rps%.1f records/second)"))

  // create an actor to transform the MongoDB results to Avro-encoded records
  lazy val transformer: ActorRef = prepareActor(new TransformingActor({
    case MongoFindResults(coll, docs) =>
      docs.flatMap(_.getAs[String]("symbol")) foreach { symbol =>
        Try {
          // retrieve the quote
          val quote = YFRealtimeStockQuoteService.getQuoteSync(symbol)

          // publish the quote to Kafka
          kafkaPublisher ! PublishAvro(kafkaTopic, toAvro(quote))

          // also write the quote to the database
          mongoActor ! Upsert(recipient = None, mongoCollection, query = O("symbol" -> symbol), doc = toMongoDoc(quote))

        } match {
          case Success(_) => counter += 1
          case Failure(e) =>
            counter -= 1
            log.error(s"Failed to process real-time quote for $symbol: ${e.getMessage}")
        }
      }
      true
    case message =>
      log.warn(s"Received unexpected message $message (${Option(message).map(_.getClass.getName).orNull})")
      false
  }), parallelism = 30)

  onStart { _ =>
    // Sends the symbols to the transforming actor, which will load the quote, transform it to Avro,
    // and send it to Kafka
    // db.Stocks.count({active:true, exchange:{$nin:['OTCBB','OTHER_OTC']}})
    val lastModified = new DateTime().minusMinutes(5)
    log.info(s"Retrieving Real-time quote symbols from collection $mongoCollection (modified since $lastModified)...")
    mongoActor ! Find(
      recipient = transformer,
      name = mongoCollection,
      query = O("active" -> true) ++ ("exchange" $nin List("OTCBB", "OTHER_OTC")) ++
        $or("yfRealTimeLastUpdated" $exists false, "yfRealTimeLastUpdated" $lte lastModified),
      fields = O("symbol" -> 1),
      maxFetchSize = 200)
  }

  private def toAvro(quote: YFRealtimeQuote): GenericRecord = {
    YahooRealTimeQuoteRecord.newBuilder()
      .setSymbol(quote.symbol)
      .setExchange(quote.exchange.orNull)
      .setAsk(quote.ask.map(n => n: JDouble).orNull)
      .setAskSize(quote.askSize.map(n => n: Integer).orNull)
      .setAvgVol3m(quote.avgVol3m.map(n => n: JLong).orNull)
      .setBid(quote.bid.map(n => n: JDouble).orNull)
      .setBidSize(quote.bidSize.map(n => n: Integer).orNull)
      .setBeta(quote.beta.map(n => n: JDouble).orNull)
      .setChange(quote.change.map(n => n: JDouble).orNull)
      .setChangePct(quote.changePct.map(n => n: JDouble).orNull)
      .setClose(quote.close.map(n => n: JDouble).orNull)
      .setDividend(quote.dividend.map(n => n: JDouble).orNull)
      .setDivYield(quote.divYield.map(n => n: JDouble).orNull)
      .setEps(quote.eps.map(n => n: JDouble).orNull)
      .setHigh(quote.high.map(n => n: JDouble).orNull)
      .setHigh52Week(quote.high52Week.map(n => n: JDouble).orNull)
      .setLastTrade(quote.lastTrade.map(n => n: JDouble).orNull)
      .setLow(quote.low.map(n => n: JDouble).orNull)
      .setLow52Week(quote.low52Week.map(n => n: JDouble).orNull)
      .setMarketCap(quote.marketCap.map(n => n: JDouble).orNull)
      .setName(quote.name.orNull)
      .setNextEarningsDate(quote.nextEarningsDate.map(n => n.getTime: JLong).orNull)
      .setOpen(quote.open.map(n => n: JDouble).orNull)
      .setPeRatio(quote.peRatio.map(n => n: JDouble).orNull)
      .setPrevClose(quote.prevClose.map(n => n: JDouble).orNull)
      .setResponseTimeMsec(quote.responseTimeMsec: JLong)
      .setSpread(quote.spread.map(n => n: JDouble).orNull)
      .setTarget1Yr(quote.target1Yr.map(n => n: JDouble).orNull)
      .setTradeDateTime(quote.tradeDateTime.map(n => n.getTime: JLong).orNull)
      .setVolume(quote.volume.map(n => n: JLong).orNull)
      .build()
  }

  private def toMongoDoc(q: YFRealtimeQuote): O = {
    $set(
      "name" -> q.name,
      "exchange" -> q.exchange,
      "lastTrade" -> q.lastTrade,
      "tradeDate" -> q.tradeDateTime,
      "tradeDateTime" -> q.tradeDateTime,
      "change" -> q.change,
      "changePct" -> q.changePct ?? computeChangePct(q.prevClose, q.lastTrade),
      "prevClose" -> q.prevClose,
      "open" -> q.open,
      "close" -> q.close,
      "ask" -> q.ask,
      "askSize" -> q.askSize,
      "bid" -> q.bid,
      "bidSize" -> q.bidSize,
      "target1Yr" -> q.target1Yr,
      "beta" -> q.beta,
      "nextEarningsDate" -> q.nextEarningsDate,
      "high" -> q.high,
      "low" -> q.low,
      "spread" -> q.spread ?? computeSpread(q.high, q.low),
      "high52Week" -> q.high52Week,
      "low52Week" -> q.low52Week,
      "volume" -> q.volume,
      "avgVol3m" -> q.avgVol3m,
      "marketCap" -> q.marketCap,
      "peRatio" -> q.peRatio,
      "eps" -> q.eps,
      "dividend" -> q.dividend,
      "divYield" -> q.divYield,

      // classification fields
      "assetType" -> "Common Stock",
      "assetClass" -> "Equity",

      // administrative fields
      "yfRealTimeRespTimeMsec" -> q.responseTimeMsec,
      "yfRealTimeLastUpdated" -> new Date())
  }

}

/**
 * Yahoo! Finance Real-time Quotes Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object YFRealTimeNarrative {
  val nonInclusion = Set("tradeDate", "tradeDateTime", "responseTimeMsec")

}
