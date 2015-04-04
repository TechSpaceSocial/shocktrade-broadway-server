package com.shocktrade.datacenter.narratives

import java.util.Properties

import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.FileReadingActor.{CopyText, Delimited, _}
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor
import com.ldaniels528.broadway.core.actors.{FileReadingActor, ThrottlingActor, ThroughputCalculatingActor}
import com.ldaniels528.broadway.core.resources._
import com.ldaniels528.broadway.core.util.PropertiesHelper._
import com.ldaniels528.broadway.server.ServerConfig
import com.shocktrade.datacenter.actors.EodDataToAvroActor
import org.slf4j.LoggerFactory

/**
 * EODData.com Import Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class EodDataImportNarrative(config: ServerConfig, id: String, props: Properties)
  extends BroadwayNarrative(config, id, props) {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  // extract the properties we need
  val kafkaTopic = props.getOrDie("kafka.topic")
  val zkConnect = props.getOrDie("zookeeper.connect")

  // create a file reader actor to read lines from the incoming resource
  lazy val fileReader = prepareActor(new FileReadingActor(config))

  // create a Kafka publishing actor
  lazy val kafkaPublisher = prepareActor(new KafkaPublishingActor(zkConnect))

  // let's calculate the throughput of the Kafka publishing actor
  var ticker = 0
  val throughputCalc = prepareActor(new ThroughputCalculatingActor(kafkaPublisher, { messagesPerSecond =>
    // log the throughput every 5 seconds
    if (ticker % 5 == 0) {
      logger.info(f"KafkaPublisher: Throughput rate is $messagesPerSecond%.1f")
    }
    ticker += 1
  }))

  // let's throttle the messages flowing into Kafka
  val throttler = prepareActor(new ThrottlingActor(throughputCalc, rateLimit = 250, enabled = true))

  // create a EOD data transformation actor
  val eodDataToAvroActor = prepareActor(new EodDataToAvroActor(kafkaTopic, throttler))

  onStart {
    _ foreach {
      case resource: ReadableResource =>
        // start the processing by submitting a request to the file reader actor
        fileReader ! CopyText(resource, eodDataToAvroActor, handler = Delimited("[,]"))
      case _ =>
        throw new IllegalStateException(s"A ${classOf[ReadableResource].getName} was expected")
    }
  }
}
