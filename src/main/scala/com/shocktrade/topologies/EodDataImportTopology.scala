package com.shocktrade.topologies

import com.ldaniels528.broadway.server.etl.BroadwayTopology
import com.ldaniels528.broadway.server.etl.BroadwayTopology.Implicits._
import com.ldaniels528.broadway.server.etl.actors.FileReadingActor
import com.ldaniels528.broadway.server.etl.actors.FileReadingActor._
import com.ldaniels528.broadway.server.etl.actors.kafka.avro.KafkaAvroPublishingActor
import com.shocktrade.actors.{EodDataEnrichmentActor, KafkaConstants}

/**
 * EODData.com Import Topology
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class EodDataImportTopology() extends BroadwayTopology("EODData.com Import") with KafkaConstants {

  onStart { resource =>
    // create a file reader actor to read lines from the incoming resource
    val fileReader = addActor(new FileReadingActor())

    // create a Kafka publishing actor
    val kafkaPublisher = addActor(new KafkaAvroPublishingActor(eodDataTopic, brokers))

    // create a EOD data transformation actor
    val transformerActor = addActor(new EodDataEnrichmentActor(kafkaPublisher))

    // start the processing by submitting a request to the file reader actor
    fileReader ! CopyText(resource, transformerActor, handler = Delimited("[,]"))
  }
}