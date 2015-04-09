package com.shocktrade.datacenter.narratives.stock.yahoo.csv

import java.lang.{Double => JDouble}
import java.util.{Date, Properties}

import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.TransformingActor
import com.ldaniels528.broadway.core.actors.kafka.KafkaConsumingActor
import com.ldaniels528.broadway.core.actors.kafka.KafkaConsumingActor.{AvroMessageReceived, StartConsuming, StopConsuming}
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor.{Upsert, _}
import com.ldaniels528.broadway.core.util.PropertiesHelper._
import com.ldaniels528.broadway.server.ServerConfig
import com.ldaniels528.trifecta.util.OptionHelper._
import com.mongodb.casbah.Imports.{DBObject => O, _}
import com.shocktrade.avro.CSVQuoteRecord
import org.slf4j.LoggerFactory

/**
 * CSV Stock Quotes: Kafka/Avro to MongoDB Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class YFCsvKafkaToDBNarrative(config: ServerConfig, id: String, props: Properties)
  extends BroadwayNarrative(config, id, props) {
  lazy val log = LoggerFactory.getLogger(getClass)

  // extract the properties we need
  val kafkaTopic = props.getOrDie("kafka.topic")
  val mongoReplicas = props.getOrDie("mongo.replicas")
  val mongoDatabase = props.getOrDie("mongo.database")
  val mongoCollection = props.getOrDie("mongo.collection")
  val zkConnect = props.getOrDie("zookeeper.connect")

  // create a MongoDB actor for persisting stock quotes
  lazy val mongoWriter = prepareActor(MongoDBActor(parseServerList(mongoReplicas), mongoDatabase), parallelism = 2)

  // create the Kafka message consumer
  lazy val kafkaConsumer = prepareActor(new KafkaConsumingActor(zkConnect), parallelism = 1)

  // create an actor to persist the Avro-encoded stock records to MongoDB
  lazy val transformer = prepareActor(new TransformingActor({
    case AvroMessageReceived(topic, partition, offset, key, message: CSVQuoteRecord) => persistQuote(message)
    case MongoWriteResult(coll, doc, result, refObj) => updateNewSymbol(refObj)
    case message =>
      log.warn(s"Received unexpected message $message (${Option(message).map(_.getClass.getName).orNull})")
      false
  }))

  // finally, start the process by initiating the consumption of Avro stock records
  onStart { resource =>
    kafkaConsumer ! StartConsuming(kafkaTopic, transformer, Some(CSVQuoteRecord.getClassSchema))
  }

  // stop consuming messages when the narrative is deactivated
  onStop { () =>
    kafkaConsumer ! StopConsuming(kafkaTopic, transformer)
  }

  private def persistQuote(q: CSVQuoteRecord): Boolean = {
    val newSymbol = Option(q.getNewSymbol)
    val oldSymbol = Option(q.getSymbol) // q.oldSymbol
    val theSymbol = newSymbol ?? oldSymbol

    mongoWriter ! Upsert(
      transformer,
      mongoCollection,
      query = O("symbol" -> theSymbol),
      doc = $set(
        "exchange" -> q.getExchange,
        "lastTrade" -> q.getLastTrade,
        "tradeDate" -> q.getTradeDate,
        "tradeDateTime" -> q.getTradeDateTime,
        "ask" -> q.getAsk,
        "bid" -> q.getBid,
        "prevClose" -> q.getPrevClose,
        "open" -> q.getOpen,
        "close" -> q.getClose,
        "change" -> q.getChange,
        "changePct" -> Option(q.getChangePct) ?? computeChangePct(Option(q.getPrevClose), Option(q.getLastTrade)),
        "high" -> q.getHigh,
        "low" -> q.getLow,
        "spread" -> computeSpread(Option(q.getHigh), Option(q.getLow)),
        "volume" -> q.getVolume,

        // classification fields
        "assetType" -> "Common Stock",
        "assetClass" -> "Equity",

        // administrative fields
        "yfDynRespTimeMsec" -> q.getResponseTimeMsec,
        "yfDynLastUpdated" -> new Date(),
        "lastUpdated" -> new Date()),
      refObj = Some(q))
    true
  }

  private def updateNewSymbol(refObj: Option[Any]): Boolean = {
    refObj foreach { case record: CSVQuoteRecord =>
      val newSymbol = Option(record.getNewSymbol)
      val oldSymbol = Option(record.getSymbol) // record.oldSymbol
      newSymbol.foreach { symbol =>
        mongoWriter ! Upsert(transformer, mongoCollection, query = O("symbol" -> oldSymbol), doc = O("symbol" -> oldSymbol))
        mongoWriter ! Upsert(transformer, mongoCollection, query = O("symbol" -> symbol), doc = $set("oldSymbol" -> oldSymbol))
      }
    }
    true
  }

  private def computeSpread(high: Option[JDouble], low: Option[JDouble]) = {
    for {
      hi <- high
      lo <- low
    } yield if (lo != 0.0d) 100d * (hi - lo) / lo else 0.0d
  }

  private def computeChangePct(prevClose: Option[JDouble], lastTrade: Option[JDouble]): Option[JDouble] = {
    for {
      prev <- prevClose
      last <- lastTrade
      diff = last - prev
    } yield (if (diff != 0) 100d * (diff / prev) else 0.0d): JDouble
  }

}

