package com.shocktrade.datacenter.narratives.securities.yahoo.csv

import java.lang.{Double => JDouble, Long => JLong}
import java.util.{Date, Properties}

import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.TransformingActor
import com.ldaniels528.broadway.core.actors.kafka.KafkaConsumingActor
import com.ldaniels528.broadway.core.actors.kafka.KafkaConsumingActor._
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor.{Upsert, _}
import com.ldaniels528.broadway.core.util.Counter
import com.ldaniels528.broadway.core.util.PropertiesHelper._
import com.ldaniels528.broadway.datasources.avro.AvroUtil._
import com.ldaniels528.broadway.server.ServerConfig
import com.mongodb.casbah.Imports.{DBObject => O, _}
import com.shocktrade.avro.CSVQuoteRecord
import com.shocktrade.datacenter.narratives.securities.StockQuoteSupport
import org.apache.avro.generic.GenericRecord

import scala.collection.JavaConversions._
import scala.concurrent.duration._

/**
 * CSV Stock Quotes: Kafka/Avro to MongoDB Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class YFCsvKafkaToDBNarrative(config: ServerConfig, id: String, props: Properties)
  extends BroadwayNarrative(config, id, props)
  with StockQuoteSupport {

  // extract the properties we need
  val kafkaTopic = props.getOrDie("kafka.topic")
  val kafkaGroupId = props.getOrDie("kafka.group")
  val topicParallelism = props.getOrDie("kafka.topic.parallelism").toInt
  val mongoReplicas = props.getOrDie("mongo.replicas")
  val mongoDatabase = props.getOrDie("mongo.database")
  val mongoCollection = props.getOrDie("mongo.collection")
  val zkConnect = props.getOrDie("zookeeper.connect")

  // create a MongoDB actor for persisting stock quotes
  lazy val mongoWriter = prepareActor(MongoDBActor(parseServerList(mongoReplicas), mongoDatabase), id = "mongoWriter", parallelism = 10)

  // create the Kafka message consumer
  lazy val kafkaConsumer = prepareActor(new KafkaConsumingActor(zkConnect), id = "kafkaConsumer", parallelism = 1)

  // create a counter for statistics
  val counter = new Counter(1.minute)((delta, rps) => log.info(f"$kafkaTopic -> $mongoCollection: $delta records ($rps%.1f records/second)"))

  // create an actor to persist the Avro-encoded stock records to MongoDB
  lazy val transformer = prepareActor(new TransformingActor({
    case AvroMessageReceived(topic, partition, offset, key, message) => persistQuote(message)
    case MongoWriteResult(coll, doc, result, refObj) => updateNewSymbol(refObj)
    case message =>
      log.warn(s"Received unexpected message $message (${Option(message).map(_.getClass.getName).orNull})")
      false
  }), parallelism = 20)

  // finally, start the process by initiating the consumption of Avro stock records
  onStart { resource =>
    kafkaConsumer ! StartConsuming(kafkaTopic, kafkaGroupId, transformer, Some(CSVQuoteRecord.getClassSchema))
  }

  // stop consuming messages when the narrative is deactivated
  onStop { () =>
    kafkaConsumer ! StopConsuming(kafkaTopic, kafkaGroupId, transformer)
  }

  private def persistQuote(rec: GenericRecord): Boolean = {
    rec.asOpt[String]("symbol") foreach { symbol =>
      val fieldNames = rec.getSchema.getFields.map(_.name).filterNot(f => f == "tradeDate" || f == "tradeDateTime").toSeq
      val changePct = rec.asOpt[JDouble]("changePct")
      val prevClose = rec.asOpt[JDouble]("prevClose")
      val lastTrade = rec.asOpt[JDouble]("lastTrade")
      val high = rec.asOpt[JDouble]("high")
      val low = rec.asOpt[JDouble]("low")
      val tradeDateTime = rec.asOpt[JLong]("tradeDateTime") map(new Date(_))

      // build the document
      val doc = rec.toMongoDB(fieldNames) ++ O(
        // calculated fields
        "changePct" -> (changePct getOrElse computeChangePct(prevClose, lastTrade)),
        "spread" -> computeSpread(high, low),
        "tradeDateTime" -> tradeDateTime,
        "tradeDate" -> tradeDateTime,

        // classification fields
        "assetType" -> "Common Stock",
        "assetClass" -> "Equity",

        // administrative fields
        "yfDynRespTimeMsec" -> rec.asOpt[JLong]("responseTimeMsec"),
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

  private def updateNewSymbol(refObj: Option[Any]): Boolean = {
    refObj foreach { case rec: GenericRecord =>
      for {
         newSymbol <- rec.asOpt[String] ("newSymbol")
         oldSymbol <- rec.asOpt[String] ("symbol") // record.oldSymbol
      } {
        mongoWriter ! Upsert(transformer, mongoCollection, query = O("symbol" -> oldSymbol), doc = O("symbol" -> oldSymbol))
        mongoWriter ! Upsert(transformer, mongoCollection, query = O("symbol" -> newSymbol), doc = $set("oldSymbol" -> oldSymbol))
      }
    }
    true
  }

}

