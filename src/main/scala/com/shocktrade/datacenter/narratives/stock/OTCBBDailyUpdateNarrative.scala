package com.shocktrade.datacenter.narratives.stock

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.file.FileReadingActor
import com.ldaniels528.broadway.core.actors.file.FileReadingActor._
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor.PublishAvro
import com.ldaniels528.broadway.core.resources.ReadableResource
import com.ldaniels528.broadway.core.util.PropertiesHelper._
import com.ldaniels528.broadway.core.util.TextFileHelper._
import com.ldaniels528.broadway.server.ServerConfig
import com.ldaniels528.trifecta.io.avro.AvroConversion
import com.shocktrade.avro.OTCTransHistoryRecord
import com.shocktrade.datacenter.narratives.stock.OTCBBDailyUpdateNarrative.OTCDailyRecord
import org.joda.time.format.DateTimeFormat

/**
 * OTC Bulletin Board Daily Update Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class OTCBBDailyUpdateNarrative(config: ServerConfig, id: String, props: Properties)
  extends BroadwayNarrative(config, id, props) {
  private val dfTs = DateTimeFormat.forPattern("MM/dd/yyyy hh:mm:ss")
  private val dfDt = DateTimeFormat.forPattern("MM/dd/yyyy")

  // extract the properties we need
  private val kafkaTopic = props.getOrDie("kafka.topic")
  private val zkConnect = props.getOrDie("zookeeper.connect")

  // create a file reader actor to read lines from the incoming resource
  lazy val fileReader = prepareActor(new FileReadingActor(config), parallelism = 10)

  // create a Kafka publishing actor for OTC transactions
  lazy val kafkaPublisher = prepareActor(new KafkaPublishingActor(zkConnect), parallelism = 10)

  onStart {
    case Some(resource: ReadableResource) =>
      fileReader ! TransformFile(resource, kafkaPublisher, transform)
    case _ =>
  }

  private def transform(lineNo: Long, line: String) = {
    if (lineNo == 1) None
    else {
      // start the processing by submitting a request to the file reader actor
      val tokens = parseTokens(line, "[,]")
      val items = tokens map (s => if (s.isEmpty || s == "**") None else Some(s))
      val builder = OTCTransHistoryRecord.newBuilder()
      val transaction = OTCDailyRecord(
        items.head map dfTs.parseDateTime map (_.toDate),
        items(1),
        items(2),
        items(3),
        items(4),
        items(5),
        items(6) map dfDt.parseDateTime map (_.toDate),
        items(7),
        items(8),
        items(9),
        items(10),
        items(11) map (_ == "Y"),
        items(12) map (_.toInt),
        items(13) map (_ == "Y"))
      AvroConversion.copy(transaction, builder)
      Some(PublishAvro(kafkaTopic, builder.build()))
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

/**
 * OTC Bulletin Board Daily Update Narrative Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object OTCBBDailyUpdateNarrative {

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