package com.shocktrade.datacenter.narratives.securities.yahoo.keystats

import java.lang.{Long => JLong}
import java.util.{Date, Properties}

import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.BroadwayActor.Implicits._
import com.ldaniels528.broadway.core.actors.TransformingActor
import com.ldaniels528.broadway.core.actors.kafka.KafkaConsumingActor
import com.ldaniels528.broadway.core.actors.kafka.KafkaConsumingActor._
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor.{Upsert, _}
import com.ldaniels528.broadway.core.util.Counter
import com.ldaniels528.broadway.datasources.avro.AvroUtil._
import com.ldaniels528.broadway.server.ServerConfig
import com.ldaniels528.commons.helpers.PropertiesHelper._
import com.mongodb.casbah.Imports.{DBObject => O, _}
import com.shocktrade.avro.KeyStatisticsRecord
import org.apache.avro.generic.GenericRecord

import scala.collection.JavaConversions._
import scala.concurrent.duration._

/**
 * Yahoo! Finance Key Statistics: Kafka to MongoDB Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class YFKeyStatisticsKafkaToDBNarrative(config: ServerConfig, id: String, props: Properties)
  extends BroadwayNarrative(config, id, props) {

  // extract the properties we need
  val kafkaTopic = props.getOrDie("kafka.topic")
  val kafkaGroupId = props.getOrDie("kafka.group")
  val topicParallelism = props.getOrDie("kafka.topic.parallelism").toInt
  val mongoReplicas = props.getOrDie("mongo.replicas")
  val mongoDatabase = props.getOrDie("mongo.database")
  val mongoCollection = props.getOrDie("mongo.collection")
  val mongoParallelism = props.getOrDie("mongo.parallelism").toInt
  val zkConnect = props.getOrDie("zookeeper.connect")

  // create a counter for statistics
  val counter = new Counter(1.minute)((successes, failures, rps) =>
    log.info(f"$kafkaTopic -> $mongoCollection: $successes records, $failures failures ($rps%.1f records/second)"))

  // create a MongoDB actor for persisting stock quotes
  lazy val mongoWriter = prepareActor(MongoDBActor(parseServerList(mongoReplicas), mongoDatabase), id = "mongoWriter", parallelism = mongoParallelism)

  // create the Kafka message consumer
  lazy val kafkaConsumer = prepareActor(actor = new KafkaConsumingActor(zkConnect))

  // create an actor to persist the Avro-encoded stock records to MongoDB
  lazy val transformer = prepareActor(new TransformingActor({
    case AvroMessageReceived(topic, partition, offset, key, record) => persistKeyStatistics(record)
    case _: MongoWriteResult => true
    case message =>
      log.warn(s"Received unexpected message $message (${Option(message).map(_.getClass.getName).orNull})")
      false
  }), parallelism = 20)

  // finally, start the process by initiating the consumption of Avro stock records
  onStart { resource =>
    kafkaConsumer ! StartConsuming(kafkaTopic, kafkaGroupId, transformer, Some(KeyStatisticsRecord.getClassSchema))
  }

  // stop consuming messages when the narrative is deactivated
  onStop { () =>
    kafkaConsumer ! StopConsuming(kafkaTopic, kafkaGroupId, transformer)
  }

  private def persistKeyStatistics(rec: GenericRecord): Boolean = {
    rec.asOpt[String]("symbol") foreach { symbol =>
      val fieldNames = rec.getSchema.getFields.map(_.name).toSeq

      // build the document
      val doc = rec.toMongoDB(fieldNames) ++ O(
        // administrative fields
        "yfKeyStatsRespTimeMsec" -> rec.asOpt[JLong]("responseTimeMsec"),
        "yfKeyStatsLastUpdated" -> new Date()
      )

      mongoWriter ! Upsert(transformer, mongoCollection, query = O("symbol" -> symbol), doc = $set(doc.toSeq: _*), refObj = Some(rec))
      counter += 1
    }
    true
  }

}
