package com.shocktrade.topologies

import com.ldaniels528.broadway.server.etl.BroadwayTopology
import com.ldaniels528.broadway.server.etl.actors.FileReadingActor.{Delimited, TextParse}
import com.ldaniels528.broadway.server.etl.actors.{KafkaAvroPublishingActor, FileReadingActor}
import com.shocktrade.actors.{KafkaConstants, KeyStatisticsLookupActor}

/**
 * ShockTrade Key Statistics Import Topology
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KeyStatsImportTopology() extends BroadwayTopology("Key Statistics Import Topology") with KafkaConstants {

  onStart { resource =>
    // create a file reader actor to read lines from the incoming resource
    val fileReader = addActor(new FileReadingActor())

    // create a Kafka publishing actor for stock quotes
    val keyStatsPublisher = addActor(new KafkaAvroPublishingActor(keyStatsTopic, brokers))

    // create a stock quote lookup actor
    val keyStatsLookup = addActor(new KeyStatisticsLookupActor(keyStatsPublisher))

    // start the processing by submitting a request to the file reader actor
    fileReader ! TextParse(resource, Delimited("\t"), keyStatsLookup)
  }
}
