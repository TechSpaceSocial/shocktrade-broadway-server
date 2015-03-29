package com.shocktrade.narratives

import java.lang.{Double => JDouble, Long => JLong}
import java.text.SimpleDateFormat

import akka.actor.{Actor, ActorRef}
import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.FileReadingActor.{CopyText, Delimited, _}
import com.ldaniels528.broadway.core.actors.kafka.avro.KafkaAvroPublishingActor
import com.ldaniels528.broadway.core.actors.kafka.avro.KafkaAvroPublishingActor._
import com.ldaniels528.broadway.core.actors.{FileReadingActor, ThrottlingActor, ThroughputCalculatingActor}
import com.ldaniels528.broadway.core.resources._
import com.ldaniels528.broadway.server.ServerConfig
import com.ldaniels528.trifecta.util.StringHelper._
import com.shocktrade.helpers.ConversionHelper._
import com.shocktrade.helpers.ResourceTracker
import com.shocktrade.narratives.EodDataImportNarrative._
import org.slf4j.LoggerFactory

/**
 * EODData.com Import Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class EodDataImportNarrative(config: ServerConfig) extends BroadwayNarrative(config, "EOD Data Import")
with KafkaConstants {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  // create a file reader actor to read lines from the incoming resource
  val fileReader = addActor(new FileReadingActor(config))

  // create a Kafka publishing actor
  val kafkaPublisher = addActor(new KafkaAvroPublishingActor(eodDataTopic, brokers))

  // let's calculate the throughput of the Kafka publishing actor
  var ticker = 0
  val throughputCalc = addActor(new ThroughputCalculatingActor(kafkaPublisher, { messagesPerSecond =>
    // log the throughput every 5 seconds
    if (ticker % 5 == 0) {
      logger.info(f"KafkaPublisher: Throughput rate is $messagesPerSecond%.1f")
    }
    ticker += 1
  }))

  // let's throttle the messages flowing into Kafka
  val throttler = addActor(new ThrottlingActor(throughputCalc, rateLimit = 250, enabled = true))

  // create a EOD data transformation actor
  val eodDataToAvroActor = addActor(new EodDataToAvroActor(throttler))

  onStart {
    case resource: ReadableResource =>
      // start the processing by submitting a request to the file reader actor
      fileReader ! CopyText(resource, eodDataToAvroActor, handler = Delimited("[,]"))
    case _ =>
      throw new IllegalStateException(s"A ${classOf[ReadableResource].getName} was expected")
  }
}

/**
 * EODData.com Import Narrative Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object EodDataImportNarrative {

  /**
   * EODData-to-Avro Actor
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  class EodDataToAvroActor(kafkaActor: ActorRef) extends Actor {
    private val sdf = new SimpleDateFormat("yyyyMMdd")

    override def receive = {
      case OpeningFile(resource) =>
        ResourceTracker.start(resource)

      case ClosingFile(resource) =>
        ResourceTracker.stop(resource)

      case TextLine(resource, lineNo, line, tokens) =>
        // skip the header line
        if (lineNo != 1) {
          kafkaActor ! PublishAvro(record = toAvro(resource, tokens))
        }

      case message =>
        unhandled(message)
    }

    /**
     * Converts the given tokens into an Avro record
     * @param tokens the given tokens
     * @return an Avro record
     */
    private def toAvro(resource: ReadableResource, tokens: Seq[String]) = {
      val items = tokens map (_.trim) map (s => if (s.isEmpty) None else Some(s))
      def item(index: Int) = if (index < items.length) items(index) else None

      com.shocktrade.avro.EodDataRecord.newBuilder()
        .setSymbol(item(0).orNull)
        .setExchange(resource.getResourceName.flatMap(extractExchange).orNull)
        .setTradeDate(item(1).flatMap(_.asEPOC(sdf)).map(n => n: JLong).orNull)
        .setOpen(item(2).flatMap(_.asDouble).map(n => n: JDouble).orNull)
        .setHigh(item(3).flatMap(_.asDouble).map(n => n: JDouble).orNull)
        .setLow(item(4).flatMap(_.asDouble).map(n => n: JDouble).orNull)
        .setClose(item(5).flatMap(_.asDouble).map(n => n: JDouble).orNull)
        .setVolume(item(6).flatMap(_.asLong).map(n => n: JLong).orNull)
        .build()
    }

    /**
     * Extracts the stock exchange from the given file name
     * @param name the given file name (e.g. "NASDAQ_20120206.txt")
     * @return an option of the stock exchange (e.g. "NASDAQ")
     */
    private def extractExchange(name: String) = name.indexOptionOf("_") map (name.substring(0, _))

  }

}