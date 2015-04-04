package com.shocktrade.datacenter.narratives.yahoo

import java.util.Properties

import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.file.FileReadingActor
import FileReadingActor._
import com.ldaniels528.broadway.core.actors.file.FileReadingActor
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor
import com.ldaniels528.broadway.core.resources.ReadableResource
import com.ldaniels528.broadway.core.util.PropertiesHelper._
import com.ldaniels528.broadway.server.ServerConfig
import com.shocktrade.datacenter.actors.yahoo.KeyStatisticsLookupActor

/**
 * Yahoo! Finance Key Statistics Import Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class YFKeyStatsImportNarrative(config: ServerConfig, id: String, props: Properties)
  extends BroadwayNarrative(config, id, props) {

  // extract the properties we need
  val kafkaTopic = props.getOrDie("kafka.topic")
  val zkConnect = props.getOrDie("zookeeper.connect")

  // create a file reader actor to read lines from the incoming resource
  lazy val fileReader = prepareActor(new FileReadingActor(config))

  // create a Kafka publishing actor for stock quotes
  lazy val keyStatsPublisher = prepareActor(new KafkaPublishingActor(zkConnect))

  // create a stock quote lookup actor
  lazy val keyStatsLookup = prepareActor(new KeyStatisticsLookupActor(kafkaTopic, keyStatsPublisher))

  onStart {
    _ foreach {
      case resource: ReadableResource =>
        // start the processing by submitting a request to the file reader actor
        fileReader ! CopyText(resource, keyStatsLookup, handler = Delimited("[\t]"))
      case _ =>
        throw new IllegalStateException(s"A ${classOf[ReadableResource].getName} was expected")
    }
  }
}
