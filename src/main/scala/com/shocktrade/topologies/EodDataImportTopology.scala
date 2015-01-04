package com.shocktrade.topologies

import com.ldaniels528.broadway.BroadwayTopology
import com.ldaniels528.broadway.core.actors.Actors.Implicits._
import com.ldaniels528.broadway.core.actors.FileReadingActor
import com.ldaniels528.broadway.core.actors.FileReadingActor.{CopyText, Delimited}
import com.ldaniels528.broadway.core.actors.kafka.avro.KafkaAvroPublishingActor
import com.ldaniels528.broadway.server.ServerConfig
import com.shocktrade.actors.{EodDataEnrichmentActor, KafkaConstants}

/**
 * EODData.com Import Topology
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class EodDataImportTopology(config: ServerConfig) extends BroadwayTopology(config, "EODData.com Import") with KafkaConstants {

  onStart { resource =>
    // create a file reader actor to read lines from the incoming resource
    val fileReader = config.addActor(new FileReadingActor(config))

    // create a Kafka publishing actor
    val kafkaPublisher = config.addActor(new KafkaAvroPublishingActor(eodDataTopic, brokers))

    // create a EOD data transformation actor
    val transformerActor = config.addActor(new EodDataEnrichmentActor(kafkaPublisher))

    // start the processing by submitting a request to the file reader actor
    fileReader ! CopyText(resource, transformerActor, handler = Delimited("[,]"))
  }
}