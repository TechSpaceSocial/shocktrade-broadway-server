package com.shocktrade.datacenter.narratives.stock.yahoo.csv

import java.lang.{Double => JDouble, Long => JLong}
import java.util.Properties

import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.TransformingActor
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor.PublishAvro
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor._
import com.ldaniels528.broadway.core.util.PropertiesHelper._
import com.ldaniels528.broadway.server.ServerConfig
import com.mongodb.casbah.Imports._
import com.shocktrade.avro.CSVQuoteRecord
import com.shocktrade.datacenter.narratives.stock.ShockTradeSymbolQuerying
import com.shocktrade.services.YFStockQuoteService
import com.shocktrade.services.YFStockQuoteService.YFStockQuote
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

/**
 * CSV Stock Quotes: Yahoo! Finance to Kafka Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class YFCsvSvcToKafkaNarrative(config: ServerConfig, id: String, props: Properties)
  extends BroadwayNarrative(config, id, props)
  with ShockTradeSymbolQuerying {
  lazy val log = LoggerFactory.getLogger(getClass)
  val parameters = YFStockQuoteService.getParams(
    "symbol", "exchange", "lastTrade", "tradeDate", "tradeTime", "change", "changePct", "prevClose", "open", "close",
    "high", "low", "high52Week", "low52Week", "volume", "marketCap", "errorMessage", "ask", "askSize", "bid", "bidSize")

  // extract the properties we need
  val kafkaTopic = props.getOrDie("kafka.kafkaTopic")
  val mongoReplicas = props.getOrDie("mongo.replicas")
  val mongoDatabase = props.getOrDie("mongo.database")
  val mongoCollection = props.getOrDie("mongo.collection")
  val zkConnect = props.getOrDie("zookeeper.connect")

  // create a MongoDB actor for retrieving stock quotes
  lazy val mongoReader = prepareActor(MongoDBActor(parseServerList(mongoReplicas), mongoDatabase), parallelism = 1)

  // create a Kafka publishing actor for stock quotes
  // NOTE: the Kafka parallelism is equal to the number of brokers
  lazy val kafkaPublisher = prepareActor(new KafkaPublishingActor(zkConnect), parallelism = 6)

  // create an actor to transform the MongoDB results to Avro-encoded records
  lazy val transformer = prepareActor(new TransformingActor({
    case MongoFindResults(coll, docs) =>
      val symbols = docs.flatMap(_.getAs[String]("symbol"))
      YFStockQuoteService.getQuotesSync(symbols, parameters) foreach { quote =>
        kafkaPublisher ! PublishAvro(kafkaTopic, toAvro(quote))
      }
      true
    case _ => false
  }))

  onStart { resource =>
    // 1. Query all symbols not update in the last 5 minutes
    // 2. Send the symbols to the transforming actor, which will load the quote, transform it to Avro
    // 3. Write each Avro record to Kafka
    mongoReader ! symbolLookupQuery(transformer, mongoCollection, new DateTime().minusMinutes(5), fetchSize = 32)
  }

  private def toAvro(quote: YFStockQuote) = {
    CSVQuoteRecord.newBuilder()
      .setSymbol(quote.symbol)
      .setExchange(quote.exchange.orNull)
      .setAsk(quote.ask.map(n => n: JDouble).orNull)
      .setAskSize(quote.askSize.map(n => n: Integer).orNull)
      .setBid(quote.bid.map(n => n: JDouble).orNull)
      .setBidSize(quote.bidSize.map(n => n: Integer).orNull)
      .setChange(quote.change.map(n => n: JDouble).orNull)
      .setChangePct(quote.changePct.map(n => n: JDouble).orNull)
      .setClose(quote.close.map(n => n: JDouble).orNull)
      .setHigh(quote.high.map(n => n: JDouble).orNull)
      .setHigh52Week(quote.high52Week.map(n => n: JDouble).orNull)
      .setLastTrade(quote.lastTrade.map(n => n: JDouble).orNull)
      .setLow(quote.low.map(n => n: JDouble).orNull)
      .setLow52Week(quote.low52Week.map(n => n: JDouble).orNull)
      .setMarketCap(quote.marketCap.map(n => n: JDouble).orNull)
      .setName(quote.name.orNull)
      .setOpen(quote.open.map(n => n: JDouble).orNull)
      .setPrevClose(quote.prevClose.map(n => n: JDouble).orNull)
      .setResponseTimeMsec(quote.responseTimeMsec: JLong)
      .setTradeDateTime(quote.tradeDateTime.map(n => n.getTime: JLong).orNull)
      .setVolume(quote.volume.map(n => n: JLong).orNull)
      .build()
  }

  private def computeSpread(high: Option[Double], low: Option[Double]): Option[Double] = {
    for {
      hi <- high
      lo <- low
    } yield if (lo != 0.0d) (hi - lo) / lo else 0.0d
  }

  private def computeChangePct(prevClose: Option[Double], lastTrade: Option[Double]) = {
    for {
      prev <- prevClose
      last <- lastTrade
      diff = last - prev
    } yield if (diff != 0) 100d * (diff / prev) else 0.0d
  }

}