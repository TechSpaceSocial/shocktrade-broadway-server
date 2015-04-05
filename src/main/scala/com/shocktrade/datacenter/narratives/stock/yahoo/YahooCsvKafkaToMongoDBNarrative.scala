package com.shocktrade.datacenter.narratives.stock.yahoo

import java.util.Properties

import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.kafka.KafkaConsumingActor
import com.ldaniels528.broadway.core.actors.kafka.KafkaConsumingActor.StartConsuming
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor._
import com.ldaniels528.broadway.core.util.PropertiesHelper._
import com.ldaniels528.broadway.server.ServerConfig
import com.shocktrade.datacenter.actors.yahoo.CSVQuoteTransformActor

/**
 * CSV Stock Quotes: Kafka to MongoDB Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class YahooCsvKafkaToMongoDBNarrative(config: ServerConfig, id: String, props: Properties)
  extends BroadwayNarrative(config, id, props) {

  // extract the properties we need
  val kafkaTopic = props.getOrDie("kafka.topic")
  val mongoReplicas = props.getOrDie("mongo.replicas")
  val mongoDatabase = props.getOrDie("mongo.database")
  val mongoCollection = props.getOrDie("mongo.collection")
  val zkConnect = props.getOrDie("zookeeper.connect")

  // create a MongoDB actor for persisting stock quotes
  lazy val mongoWriter = prepareActor(MongoDBActor(parseServerList(mongoReplicas), mongoDatabase), parallelism = 5)

  // create a CSV to Stock Quote object transforming actor
  lazy val quoteParser = prepareActor(new CSVQuoteTransformActor(mongoCollection, mongoWriter), parallelism = 1)

  // create the Kafka message consumer
  lazy val kafkaConsumer = prepareActor(new KafkaConsumingActor(zkConnect), parallelism = 1)

  onStart { resource =>
    kafkaConsumer ! StartConsuming(kafkaTopic, quoteParser)
  }

}

