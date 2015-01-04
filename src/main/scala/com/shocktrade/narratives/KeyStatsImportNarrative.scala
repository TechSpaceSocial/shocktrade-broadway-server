package com.shocktrade.narratives

import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.Actors.Implicits._
import com.ldaniels528.broadway.core.actors.FileReadingActor
import com.ldaniels528.broadway.core.actors.FileReadingActor.{CopyText, Delimited}
import com.ldaniels528.broadway.core.actors.kafka.avro.KafkaAvroPublishingActor
import com.ldaniels528.broadway.server.ServerConfig
import com.shocktrade.actors.{KafkaConstants, KeyStatisticsLookupActor}

/**
 * ShockTrade Key Statistics Import Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KeyStatsImportNarrative(config: ServerConfig) extends BroadwayNarrative(config, "Key Statistics Import") with KafkaConstants {

  onStart { resource =>

    implicit val ec = config.system.dispatcher

    // create a file reader actor to read lines from the incoming resource
    val fileReader = config.addActor(new FileReadingActor(config))

    // create a Kafka publishing actor for stock quotes
    val keyStatsPublisher = config.addActor(new KafkaAvroPublishingActor(keyStatsTopic, brokers))

    // create a stock quote lookup actor
    val keyStatsLookup = config.addActor(new KeyStatisticsLookupActor(keyStatsPublisher))

    // start the processing by submitting a request to the file reader actor
    fileReader ! CopyText(resource, keyStatsLookup, handler = Delimited("[\t]"))
  }
}
