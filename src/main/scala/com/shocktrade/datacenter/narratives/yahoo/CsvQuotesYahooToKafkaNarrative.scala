package com.shocktrade.datacenter.narratives.yahoo

import java.util.Properties

import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor._
import com.ldaniels528.broadway.core.util.PropertiesHelper._
import com.ldaniels528.broadway.server.ServerConfig
import com.shocktrade.datacenter.actors.yahoo.CsvQuotesYahooToKafkaNarrative.RequestQuotes
import com.shocktrade.datacenter.actors.yahoo.{QuoteLookupAndPublishActor, QuoteSymbolsActor}

/**
 * CSV Stock Quotes: Yahoo! Finance to Kafka Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class CsvQuotesYahooToKafkaNarrative(config: ServerConfig, id: String, props: Properties)
  extends BroadwayNarrative(config, id, props) {

  // extract the properties we need
  val kafkaTopic = props.getOrDie("kafka.topic")
  val mongoReplicas = props.getOrDie("mongo.replicas")
  val mongoDatabase = props.getOrDie("mongo.database")
  val mongoCollection = props.getOrDie("mongo.collection")
  val zkConnect = props.getOrDie("zookeeper.connect")

  // create a MongoDB actor for retrieving stock quotes
  lazy val mongoReader = prepareActor(MongoDBActor(parseServerList(mongoReplicas), mongoDatabase), parallelism = 1)

  // create a Kafka publishing actor for stock quotes
  // NOTE: the Kafka parallelism is equal to the number of brokers
  lazy val quotePublisher = prepareActor(new KafkaPublishingActor(zkConnect), parallelism = 6)

  // create a stock quote lookup actor
  lazy val quoteLookup = prepareActor(new QuoteLookupAndPublishActor(kafkaTopic, quotePublisher), parallelism = 1)

  // create a stock symbols requesting actor
  lazy val symbolsRequester = prepareActor(new QuoteSymbolsActor(mongoReader, mongoCollection, quoteLookup), parallelism = 1)

  onStart { resource =>
    symbolsRequester ! RequestQuotes
  }
}
