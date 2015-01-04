package com.shocktrade.narratives

import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.Actors.Implicits._
import com.ldaniels528.broadway.core.actors.FileReadingActor
import com.ldaniels528.broadway.core.actors.FileReadingActor.{CopyText, Delimited}
import com.ldaniels528.broadway.core.actors.kafka.avro.KafkaAvroPublishingActor
import com.ldaniels528.broadway.server.ServerConfig
import com.shocktrade.actors.{EodDataEnrichmentActor, KafkaConstants}

/**
 * EODData.com Import Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class EodDataImportNarrative(config: ServerConfig) extends BroadwayNarrative(config, "EODData.com Import") with KafkaConstants {

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