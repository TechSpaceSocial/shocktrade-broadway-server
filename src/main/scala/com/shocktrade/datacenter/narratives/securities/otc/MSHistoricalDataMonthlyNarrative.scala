package com.shocktrade.datacenter.narratives.securities.otc

import java.lang.{Boolean => JBoolean, Long => JLong}
import java.util.Properties

import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.TransformingActor
import com.ldaniels528.broadway.core.actors.file.FileReadingActor
import com.ldaniels528.broadway.core.actors.file.FileReadingActor.{ClosingFile, CopyText, OpeningFile, TextLine}
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor.PublishAvro
import com.ldaniels528.broadway.core.resources.ReadableResource
import com.ldaniels528.broadway.core.util.Counter
import com.ldaniels528.broadway.core.util.PropertiesHelper._
import com.ldaniels528.broadway.core.util.TextFileHelper._
import com.ldaniels528.broadway.server.ServerConfig
import com.shocktrade.avro.OTCTransHistoryRecord
import org.joda.time.format.DateTimeFormat

import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * OTCE Finra MS Historical Data Monthly Resource
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class MSHistoricalDataMonthlyNarrative(config: ServerConfig, id: String, props: Properties)
  extends BroadwayNarrative(config, id, props) {
  private val dfTs = DateTimeFormat.forPattern("MM/dd/yyyy hh:mm:ss")
  private val dfDt = DateTimeFormat.forPattern("MM/dd/yyyy")

  // extract the properties we need
  private val kafkaTopic = props.getOrDie("kafka.topic")
  private val topicParallelism = props.getOrDie("kafka.topic.parallelism").toInt
  private val zkConnect = props.getOrDie("zookeeper.connect")

  // create a file reader actor to read lines from the incoming resource
  lazy val fileReader = prepareActor(new FileReadingActor(config), id = "fileReader", parallelism = 10)

  // create a Kafka publishing actor for OTC transactions
  lazy val kafkaPublisher = prepareActor(new KafkaPublishingActor(zkConnect), id = "kafkaPublisher", parallelism = topicParallelism)

  // create a counter for statistics
  val counter = new Counter(1.minute)((successes, failures, rps) =>
    log.info(f"OTC/HDM -> $kafkaTopic: $successes records, $failures failures ($rps%.1f records/second)"))

  // create an actor to transform the MongoDB results to Avro-encoded records
  lazy val transformer = prepareActor(new TransformingActor({
    case TextLine(resource, lineNo, line, tokens) =>
      if (lineNo > 1) {
        kafkaPublisher ! PublishAvro(kafkaTopic, toAvro(line))
        counter += 1
      }
      true
    case _: OpeningFile => true
    case _: ClosingFile => true
    case _ => false
  }), parallelism = 5)

  onStart {
    case Some(resource: ReadableResource) =>
      fileReader ! CopyText(resource, transformer)
    case _ =>
  }

  private def toAvro(line: String) = {
    // start the processing by submitting a request to the file reader actor
    val tokens = parseTokens(line, "[,]") map (_.trim)
    def items(n: Int) = {
      if (n >= tokens.size) None
      else {
        tokens(n) match {
          case s if s.isEmpty || s == "**" => None
          case s => Some(s)
        }
      }
    }

    // Symbol|Security Name|Market Category|Reg SHO Threshold Flag|Rule 4320
    OTCTransHistoryRecord.newBuilder()
      .setTranDate(items(0) map dfTs.parseDateTime map (_.toDate.getTime) map (n => n: JLong) orNull)
      .setTranType(items(1).orNull)
      .setNewSymbol(items(2).orNull)
      .setOldSymbol(items(3).orNull)
      .setNewName(items(4).orNull)
      .setOldName(items(5).orNull)
      .setEffDate(items(6) map dfDt.parseDateTime map (_.toDate.getTime) map (n => n: JLong) orNull)
      .setComments(items(7).orNull)
      .setNotes(items(8).orNull)
      .setCoPhone(items(9).orNull)
      .setMarketCategory(items(10).orNull)
      .setOATSReportable(items(11) map (_ == "Y") map (n => n: JBoolean) orNull)
      .setUnitOfTrade(items(12) map (_.toInt) map (n => n: Integer) orNull)
      .setRegistrationFee(items(13) map (_ == "Y") map (n => n: JBoolean) orNull)
      .build()
  }

}
