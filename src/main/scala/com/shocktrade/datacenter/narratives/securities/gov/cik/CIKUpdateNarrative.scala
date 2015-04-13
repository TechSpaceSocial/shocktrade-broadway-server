package com.shocktrade.datacenter.narratives.securities.gov.cik

import java.util.Properties

import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.TransformingActor
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor.PublishAvro
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor._
import com.ldaniels528.broadway.core.util.Counter
import com.ldaniels528.broadway.core.util.PropertiesHelper._
import com.ldaniels528.broadway.server.ServerConfig
import com.mongodb.casbah.Imports.{DBObject => O, _}
import com.shocktrade.datacenter.narratives.securities.StockQuoteSupport
import com.shocktrade.services.CikCompanySearchService
import com.shocktrade.services.CikCompanySearchService.CikInfo

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
 * CIK Number Update Process
 * @author lawrence.daniels@gmail.com
 */
class CIKUpdateNarrative(config: ServerConfig, id: String, props: Properties)
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
  lazy val mongoReader = prepareActor(MongoDBActor(parseServerList(mongoReplicas), mongoDatabase), id = "mongoReader", parallelism = 10)

  // create a Kafka publishing actor
  lazy val kafkaPublisher = prepareActor(new KafkaPublishingActor(zkConnect), id = "kafkaPublisher", parallelism = topicParallelism)

  // create a counter for statistics
  val counter = new Counter(1.minute)((delta, rps) => log.info(f"EODDATA -> $kafkaTopic: $delta records ($rps%.1f records/second)"))

  // create an actor to transform the MongoDB results to Avro-encoded records
  lazy val transformer = prepareActor(new TransformingActor({
    case MongoFindResults(coll, docs) =>
      // extract the company name and symbol from the MongoDB record
      val results = for {
        doc <- docs
        symbol <- doc.getAs[String]("symbol")
        name <- doc.getAs[String]("name")
      } yield (symbol, name)

      // now query the CIK record by company name
      results foreach { case (symbol, companyName) =>
        Try {
          toAvro(symbol, companyName, CikCompanySearchService.search(companyName))
            .foreach(kafkaPublisher ! PublishAvro(kafkaTopic, _))
        } match {
          case Success(_) => counter += 1
          case Failure(e) =>
            log.error(s"Failed to publish CIK for $companyName ($symbol): ${e.getMessage}")
        }
      }
      true
    case _ => false
  }), parallelism = 5)

  onStart { _ =>
    // Sends the symbols to the transforming actor, which will load the quote, transform it to Avro,
    // and send it to Kafka
    mongoReader ! Find(
      recipient = transformer,
      name = mongoCollection,
      fields = O("symbol" -> 1, "name" -> 1),
      query = O("active" -> true, "cikNumber" -> null),
      maxFetchSize = 50
    )
  }

  /**
   * Converts the given tokens into an Avro record
   * @param cikInfoSeq the given collection of [[CikInfo]] instances
   * @return an Avro record
   */
  private def toAvro(symbol: String, name: String, cikInfoSeq: Seq[CikInfo]) = {
    cikInfoSeq.headOption map { cikInfo =>
      gov.sec.avro.CikRecord.newBuilder()
        .setSymbol(symbol)
        .setName(name)
        .setCikNumber(cikInfo.cikNumber)
        .setCikName(cikInfo.cikName)
        .build()
    }
  }

}
