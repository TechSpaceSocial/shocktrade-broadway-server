package com.shocktrade.datacenter.narratives.stock.yahoo

import java.lang.{Double => JDouble, Long => JLong}
import java.util.{Date, Properties}

import akka.actor.ActorRef
import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.TransformingActor
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor.PublishAvro
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor._
import com.ldaniels528.broadway.core.util.PropertiesHelper._
import com.ldaniels528.broadway.server.ServerConfig
import com.mongodb.casbah.Imports.{DBObject => O, _}
import com.shocktrade.avro.YahooRealTimeQuoteRecord
import com.shocktrade.datacenter.narratives.stock.SymbolQuerying
import com.shocktrade.services.YFRealtimeStockQuoteService
import com.shocktrade.services.YFRealtimeStockQuoteService.YFRealtimeQuote
import org.joda.time.DateTime

/**
 * Yahoo! Finance Real-time Quotes Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class YahooRealTimeQuotesNarrative(config: ServerConfig, id: String, props: Properties)
  extends BroadwayNarrative(config, id, props)
  with SymbolQuerying {

  // extract the properties we need
  val kafkaTopic = props.getOrDie("kafka.topic")
  val mongoReplicas = props.getOrDie("mongo.replicas")
  val mongoDatabase = props.getOrDie("mongo.database")
  val mongoCollection = props.getOrDie("mongo.collection")
  val zkConnect = props.getOrDie("zookeeper.connect")

  // create a MongoDB actor for retrieving stock quotes
  lazy val mongoReader = prepareActor(MongoDBActor(parseServerList(mongoReplicas), mongoDatabase), parallelism = 1)

  // create a Kafka publishing actor
  lazy val kafkaPublisher = prepareActor(new KafkaPublishingActor(zkConnect), parallelism = 1)

  // create an actor to transform the MongoDB results to Avro-encoded records
  lazy val transformer = prepareActor(new TransformingActor({
    case MongoResult(doc) =>
      doc.getAs[String]("symbol") foreach { symbol =>
        kafkaPublisher ! PublishAvro(kafkaTopic, toAvro(YFRealtimeStockQuoteService.getQuoteSync(symbol)))
      }
    case _ =>
  }))

  onStart { resource =>
    // Sends the symbols to the transforming actor, which will load the quote, transform it to Avro,
    // and send it to Kafka
    mongoReader ! symbolLookupQuery(transformer, mongoCollection, new DateTime().minusMinutes(5))
  }

  private def toAvro(quote: YFRealtimeQuote) = {
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

  private def upsert(target: ActorRef, quote: YFRealtimeQuote) {
    import quote._
    Upsert(
      target,
      name = mongoCollection,
      query = O("symbol" -> symbol),
      doc = $set(
        "name" -> quote.name,
        "exchange" -> exchange,
        "lastTrade" -> lastTrade,
        "tradeDate" -> tradeDateTime,
        "tradeDateTime" -> tradeDateTime,
        "change" -> change,
        "changePct" -> changePct,
        "prevClose" -> prevClose,
        "open" -> open,
        "close" -> close,
        "ask" -> ask,
        "askSize" -> askSize,
        "bid" -> bid,
        "bidSize" -> bidSize,
        "target1Yr" -> target1Yr,
        "beta" -> beta,
        "nextEarningsDate" -> nextEarningsDate,
        "high" -> high,
        "low" -> low,
        "spread" -> spread,
        "high52Week" -> high52Week,
        "low52Week" -> low52Week,
        "volume" -> volume,
        "avgVol3m" -> avgVol3m,
        "marketCap" -> marketCap,
        "peRatio" -> peRatio,
        "eps" -> eps,
        "dividend" -> dividend,
        "divYield" -> divYield,

        // classification fields
        "assetType" -> "Common Stock",
        "assetClass" -> "Equity",

        // administrative fields
        "yfRealTimeRespTimeMsec" -> responseTimeMsec,
        "yfRealTimeLastUpdated" -> new Date()))
  }

}

