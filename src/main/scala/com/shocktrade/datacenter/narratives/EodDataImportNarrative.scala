package com.shocktrade.datacenter.narratives

import java.util.Properties

import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.FileReadingActor
import com.ldaniels528.broadway.core.actors.FileReadingActor.{CopyText, Delimited, _}
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor
import com.ldaniels528.broadway.core.resources._
import com.ldaniels528.broadway.core.util.PropertiesHelper._
import com.ldaniels528.broadway.server.ServerConfig
import com.shocktrade.datacenter.actors.EodDataToAvroActor

/**
 * EODData.com Import Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class EodDataImportNarrative(config: ServerConfig, id: String, props: Properties)
  extends BroadwayNarrative(config, id, props) {

  // extract the properties we need
  val kafkaTopic = props.getOrDie("kafka.topic")
  val zkConnect = props.getOrDie("zookeeper.connect")

  // create a file reader actor to read lines from the incoming resource
  lazy val fileReader = prepareActor(new FileReadingActor(config), parallelism = 10)

  // create a Kafka publishing actor
  lazy val kafkaPublisher = prepareActor(new KafkaPublishingActor(zkConnect), parallelism = 10)

  // create a EOD data transformation actor
  lazy val eodDataToAvroActor = prepareActor(new EodDataToAvroActor(kafkaTopic, kafkaPublisher), parallelism = 10)

  onStart {
    case Some(resource: ReadableResource) =>
      // start the processing by submitting a request to the file reader actor
      fileReader ! CopyText(resource, eodDataToAvroActor, handler = Delimited("[,]"))
    case _ =>
  }
}
