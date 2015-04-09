package com.shocktrade.datacenter.narratives.stock.yahoo.realtime

import java.lang.{Double => JDouble}
import java.util.{Date, Properties}

import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.TransformingActor
import com.ldaniels528.broadway.core.actors.kafka.KafkaConsumingActor
import com.ldaniels528.broadway.core.actors.kafka.KafkaConsumingActor.{AvroMessageReceived, StartConsuming, StopConsuming}
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor.{Upsert, _}
import com.ldaniels528.broadway.core.util.Counter
import com.ldaniels528.broadway.core.util.PropertiesHelper._
import com.ldaniels528.broadway.datasources.avro.AvroUtil._
import com.ldaniels528.broadway.server.ServerConfig
import com.mongodb.casbah.Imports.{DBObject => O, _}
import com.shocktrade.avro.YahooRealTimeQuoteRecord
import org.apache.avro.generic.GenericRecord
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.concurrent.duration._

/**
 * Yahoo Real-time Stock Quotes: Kafka/Avro to MongoDB Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class YFRealTimeKafkaToDBNarrative(config: ServerConfig, id: String, props: Properties)
  extends BroadwayNarrative(config, id, props) {
  val log = LoggerFactory.getLogger(getClass)

  // extract the properties we need
  val kafkaTopic = props.getOrDie("kafka.topic")
  val mongoReplicas = props.getOrDie("mongo.replicas")
  val mongoDatabase = props.getOrDie("mongo.database")
  val mongoCollection = props.getOrDie("mongo.collection")
  val zkConnect = props.getOrDie("zookeeper.connect")

  // create a counter for statistics
  val counter = new Counter(1.minute)((delta, rps) => log.info(f"$kafkaTopic -> $mongoCollection: $delta records ($rps%.1f records/second)"))

  // create a MongoDB actor for persisting stock quotes
  lazy val mongoWriter = prepareActor(MongoDBActor(parseServerList(mongoReplicas), mongoDatabase), parallelism = 10)

  // create the Kafka message consumer
  lazy val kafkaConsumer = prepareActor(new KafkaConsumingActor(zkConnect), parallelism = 1)

  // create an actor to persist the Avro-encoded stock records to MongoDB
  lazy val transformer = prepareActor(new TransformingActor({
    case AvroMessageReceived(topic, partition, offset, key, record) => persistQuote(record)
    case _: MongoWriteResult => true
    case message =>
      log.warn(s"Received unexpected message $message (${Option(message).map(_.getClass.getName).orNull})")
      false
  }))

  // finally, start the process by initiating the consumption of Avro stock records
  onStart { resource =>
    kafkaConsumer ! StartConsuming(kafkaTopic, transformer, Some(YahooRealTimeQuoteRecord.getClassSchema))
  }

  // stop consuming messages when the narrative is deactivated
  onStop { () =>
    kafkaConsumer ! StopConsuming(kafkaTopic, transformer)
  }

  private def persistQuote(rec: GenericRecord): Boolean = {
    rec.asOpt[String]("symbol") foreach { symbol =>
      val fieldNames = rec.getSchema.getFields.map(_.name).toSeq
      val changePct = rec.asOpt[JDouble]("changePct")
      val prevClose = rec.asOpt[JDouble]("prevClose")
      val lastTrade = rec.asOpt[JDouble]("lastTrade")
      val high = rec.asOpt[JDouble]("high")
      val low = rec.asOpt[JDouble]("low")

      // build the document
      val doc = rec.toMongoDB(fieldNames) ++ O(
        // calculated fields
        "changePct" -> (changePct getOrElse computeChangePct(prevClose, lastTrade)),
        "spread" -> computeSpread(high, low),

        // classification fields
        "assetType" -> "Common Stock",
        "assetClass" -> "Equity",

        // administrative fields
        "yfDynRespTimeMsec" -> rec.asOpt[String]("ResponseTimeMsec"),
        "yfDynLastUpdated" -> new Date(),
        "lastUpdated" -> new Date()
      )

      mongoWriter ! Upsert(
        transformer,
        mongoCollection,
        query = O("symbol" -> symbol),
        doc = $set(doc.toSeq: _*),
        refObj = Some(rec))

      counter += 1
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
