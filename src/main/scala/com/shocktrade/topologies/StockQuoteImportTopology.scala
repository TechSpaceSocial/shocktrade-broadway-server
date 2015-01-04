package com.shocktrade.topologies

import com.ldaniels528.broadway.BroadwayTopology
import com.ldaniels528.broadway.core.actors.Actors.Implicits._
import com.ldaniels528.broadway.core.actors.FileReadingActor._
import com.ldaniels528.broadway.core.actors._
import com.ldaniels528.broadway.core.actors.kafka.avro._
import com.ldaniels528.broadway.server.ServerConfig
import com.shocktrade.actors.{KafkaConstants, StockQuoteLookupActor}

/**
 * ShockTrade Stock Quote Import Topology
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class StockQuoteImportTopology(config: ServerConfig) extends BroadwayTopology(config, "Stock Quote Import") with KafkaConstants {

  onStart { resource =>

    implicit val ec = config.system.dispatcher

    // create a file reader actor to read lines from the incoming resource
    val fileReader = config.addActor(new FileReadingActor(config))

    // create a Kafka publishing actor for stock quotes
    val quotePublisher = config.addActor(new KafkaAvroPublishingActor(quotesTopic, brokers))

    // create a stock quote lookup actor
    val quoteLookup = config.addActor(new StockQuoteLookupActor(quotePublisher))

    // start the processing by submitting a request to the file reader actor
    fileReader ! CopyText(resource, quoteLookup, handler = Delimited("[\t]"))
  }
}
