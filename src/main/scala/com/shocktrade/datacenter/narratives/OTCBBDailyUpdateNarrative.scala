package com.shocktrade.datacenter.narratives

import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.{Actor, ActorRef}
import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.FileReadingActor
import com.ldaniels528.broadway.core.actors.FileReadingActor.{CopyText, Delimited, _}
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor
import com.ldaniels528.broadway.core.resources.ReadableResource
import com.ldaniels528.broadway.server.ServerConfig
import com.ldaniels528.trifecta.io.avro.AvroConversion
import com.shocktrade.avro.OTCTransHistoryRecord
import com.shocktrade.datacenter.helpers.ResourceTracker
import com.shocktrade.datacenter.narratives.OTCBBDailyUpdateNarrative.OTCBBEnrichmentActor

/**
 * OTC Bulletin Board Daily Update Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class OTCBBDailyUpdateNarrative(config: ServerConfig) extends BroadwayNarrative(config, "OTC/BB Daily Update")
with KafkaConstants {
  // create a file reader actor to read lines from the incoming resource
  lazy val fileReader = addActor(new FileReadingActor(config))

  // create a Kafka publishing actor for OTC transactions
  lazy val otcPublisher = addActor(new KafkaPublishingActor(otcTranHistoryTopic, brokers))

  // create an OTC/BB data conversion actor
  lazy val otcConverter = addActor(new OTCBBEnrichmentActor(otcPublisher))

  onStart {
    case resource: ReadableResource =>
      // start the processing by submitting a request to the file reader actor
      fileReader ! CopyText(resource, otcConverter, handler = Delimited("[|]"))
    case _ =>
      throw new IllegalStateException(s"A ${classOf[ReadableResource].getName} was expected")
  }
}

/**
 * OTC Bulletin Board Daily Update Narrative Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object OTCBBDailyUpdateNarrative {

  /**
   * Returns the resource path of the Daily List Transaction History file
   * @return the resource path (e.g. "http://www.otcbb.com/dailylist/txthistory/BB01202015.txt")
   */
  def getResourcePath: String = {
    // lookup for a new file
    val fileName = s"BB${new SimpleDateFormat("MMddyyyy").format(new Date())}.txt"
    s"http://www.otcbb.com/dailylist/txthistory/$fileName"
  }

  /**
   * OTC/BB Data Enrichment Actor
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  class OTCBBEnrichmentActor(target: ActorRef) extends Actor {
    private val sdf_ts = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss")
    private val sdf_dt = new SimpleDateFormat("MM/dd/yyyy")

    override def receive = {
      case OpeningFile(resource) =>
        ResourceTracker.start(resource)

      case ClosingFile(resource) =>
        ResourceTracker.stop(resource)

      case TextLine(resource, lineNo, line, tokens) =>
        val items = tokens map (s => if (s == "" || s == "**") None else Some(s))
        val builder = OTCTransHistoryRecord.newBuilder()
        val transaction = OTCDailyRecord(
          items(0) map sdf_ts.parse,
          items(1),
          items(2),
          items(3),
          items(4),
          items(5),
          items(6) map sdf_dt.parse,
          items(7),
          items(8),
          items(9),
          items(10),
          items(11) map (_ == "Y"),
          items(12) map (_.toInt),
          items(13) map (_ == "Y"))
        AvroConversion.copy(transaction, builder)
        target ! builder.build()

      case message =>
        unhandled(message)
    }
  }

  /**
   * OTC Daily Update
   * @author lawrence.daniels@gmail.com
   */
  case class OTCDailyRecord(tranDate: Option[Date],
                            tranType: Option[String],
                            newSymbol: Option[String],
                            oldSymbol: Option[String],
                            newName: Option[String],
                            oldName: Option[String],
                            effDate: Option[Date],
                            comments: Option[String],
                            notes: Option[String],
                            coPhone: Option[String],
                            marketCategory: Option[String],
                            OATSReportable: Option[Boolean],
                            unitOfTrade: Option[Int],
                            registrationFee: Option[Boolean]) {
    def isSymbolChange = newSymbol.isDefined
  }

}
