package com.shocktrade.datacenter.narratives

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.FileReadingActor
import com.ldaniels528.broadway.core.actors.FileReadingActor.{CopyText, Delimited, _}
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor
import com.ldaniels528.broadway.core.resources.ReadableResource
import com.ldaniels528.broadway.core.util.PropertiesHelper._
import com.ldaniels528.broadway.server.ServerConfig
import com.shocktrade.datacenter.actors.OTCBBEnrichmentActor

/**
 * OTC Bulletin Board Daily Update Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class OTCBBDailyUpdateNarrative(config: ServerConfig, id: String, props: Properties)
  extends BroadwayNarrative(config, id, props) {

  // extract the properties we need
  val kafkaTopic = props.getOrDie("kafka.topic")
  val zkConnect = props.getOrDie("zookeeper.connect")

  // create a file reader actor to read lines from the incoming resource
  lazy val fileReader = prepareActor(new FileReadingActor(config))

  // create a Kafka publishing actor for OTC transactions
  lazy val otcPublisher = prepareActor(new KafkaPublishingActor(zkConnect))

  // create an OTC/BB data conversion actor
  lazy val otcConverter = prepareActor(new OTCBBEnrichmentActor(kafkaTopic, otcPublisher))

  onStart {
    _ foreach {
      case resource: ReadableResource =>
        // start the processing by submitting a request to the file reader actor
        fileReader ! CopyText(resource, otcConverter, handler = Delimited("[|]"))
      case _ =>
        throw new IllegalStateException(s"A ${classOf[ReadableResource].getName} was expected")
    }
  }

  /**
   * Returns the resource path of the Daily List Transaction History file
   * @return the resource path (e.g. "http://www.otcbb.com/dailylist/txthistory/BB01202015.txt")
   */
  private def getResourcePath: String = {
    // TODO this should be a custom resource
    // lookup for a new file
    val fileName = s"BB${new SimpleDateFormat("MMddyyyy").format(new Date())}.txt"
    s"http://www.otcbb.com/dailylist/txthistory/$fileName"
  }

}
