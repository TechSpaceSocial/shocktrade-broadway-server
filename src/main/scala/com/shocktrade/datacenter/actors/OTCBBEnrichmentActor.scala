package com.shocktrade.datacenter.actors

import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.{Actor, ActorRef}
import com.ldaniels528.broadway.core.actors.FileReadingActor.{ClosingFile, OpeningFile, TextLine}
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor.PublishAvro
import com.ldaniels528.trifecta.io.avro.AvroConversion
import com.shocktrade.avro.OTCTransHistoryRecord
import com.shocktrade.datacenter.actors.OTCBBEnrichmentActor.OTCDailyRecord
import com.shocktrade.datacenter.helpers.ResourceTracker

/**
 * OTC/BB Data Enrichment Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class OTCBBEnrichmentActor(otcTranHistoryTopic: String, target: ActorRef) extends Actor {
  private val sdf_ts = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss")
  private val sdf_dt = new SimpleDateFormat("MM/dd/yyyy")

  override def receive = {
    case OpeningFile(resource) =>
      ResourceTracker.start(resource)

    case ClosingFile(resource) =>
      ResourceTracker.stop(resource)

    case TextLine(resource, lineNo, line, tokens) =>
      val items = tokens map (s => if (s.isEmpty || s == "**") None else Some(s))
      val builder = OTCTransHistoryRecord.newBuilder()
      val transaction = OTCDailyRecord(
        items.head map sdf_ts.parse,
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
      target ! PublishAvro(otcTranHistoryTopic, builder.build())

    case message =>
      unhandled(message)
  }
}

/**
 * OTC Bulletin Board Daily Update Narrative Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object OTCBBEnrichmentActor {

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