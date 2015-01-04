package com.shocktrade.topologies

import com.ldaniels528.broadway.core.actors.FileReadingActor._
import com.ldaniels528.broadway.core.actors._
import com.ldaniels528.broadway.core.actors.kafka.avro._
import com.ldaniels528.broadway.server.etl.BroadwayTopology
import com.ldaniels528.broadway.server.etl.BroadwayTopology.Implicits._
import com.shocktrade.actors.{KafkaConstants, StockQuoteLookupActor}

/**
 * ShockTrade Stock Quote Import Topology
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class StockQuoteImportTopology() extends BroadwayTopology("Stock Quote Import") with KafkaConstants {

  onStart { resource =>
    // create a file reader actor to read lines from the incoming resource
    val fileReader = addActor(new FileReadingActor())

    // create a Kafka publishing actor for stock quotes
    val quotePublisher = addActor(new KafkaAvroPublishingActor(quotesTopic, brokers))

    // create a stock quote lookup actor
    val quoteLookup = addActor(new StockQuoteLookupActor(quotePublisher))

    // start the processing by submitting a request to the file reader actor
    fileReader ! CopyText(resource, quoteLookup, handler = Delimited("[\t]"))
  }
}
