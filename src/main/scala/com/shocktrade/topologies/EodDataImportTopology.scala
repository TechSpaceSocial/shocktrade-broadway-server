package com.shocktrade.topologies

import com.ldaniels528.broadway.server.etl.BroadwayTopology
import com.ldaniels528.broadway.server.etl.actors.FileReadingActor.{Delimited, TextParse}
import com.ldaniels528.broadway.server.etl.actors.{FileReadingActor, KafkaAvroPublishingActor}
import com.shocktrade.actors.{KafkaConstants, StockQuoteLookupActor}

/**
 * EODData.com Import Topology
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class EodDataImportTopology() extends BroadwayTopology("EODData.com Import Topology") with KafkaConstants {

  onStart { resource =>
    // create a file reader actor to read lines from the incoming resource
    val fileReader = addActor(new FileReadingActor())

    // create a Kafka publishing actor
    val kafkaPublisher = addActor(new KafkaAvroPublishingActor(eodDataTopic, brokers))

    // create a EOD data transformation actor
    val transformerActor = addActor(new StockQuoteLookupActor(kafkaPublisher))

    // start the processing by submitting a request to the file reader actor
    fileReader ! TextParse(resource, Delimited(","), transformerActor)
  }
}