package com.shocktrade.datacenter.narratives

import java.lang.{Double => JDouble, Long => JLong}
import java.util.Properties

import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.file.FileReadingActor
import com.ldaniels528.broadway.core.actors.file.FileReadingActor._
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor.PublishAvro
import com.ldaniels528.broadway.core.resources.ReadableResource
import com.ldaniels528.broadway.core.util.PropertiesHelper._
import com.ldaniels528.broadway.core.util.TextFileHelper._
import com.ldaniels528.broadway.server.ServerConfig
import com.ldaniels528.trifecta.util.StringHelper._
import com.shocktrade.datacenter.helpers.ConversionHelper._
import org.joda.time.format.DateTimeFormat

/**
 * EODData.com Import Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class EodDataImportNarrative(config: ServerConfig, id: String, props: Properties)
  extends BroadwayNarrative(config, id, props) {
  private val df = DateTimeFormat.forPattern("yyyyMMdd")

  // extract the properties we need
  private val kafkaTopic = props.getOrDie("kafka.topic")
  private val zkConnect = props.getOrDie("zookeeper.connect")

  // create a file reader actor to read lines from the incoming resource
  lazy val fileReader = prepareActor(new FileReadingActor(config), parallelism = 10)

  // create a Kafka publishing actor
  lazy val kafkaPublisher = prepareActor(new KafkaPublishingActor(zkConnect), parallelism = 10)

  onStart {
    case Some(resource: ReadableResource) =>
      // start the processing by submitting a request to the file reader actor
      fileReader ! TransformFile(resource, kafkaPublisher, (lineNo, line) =>
        if (lineNo == 1) None else Some(PublishAvro(kafkaTopic, toAvro(resource, parseTokens(line, "[,]")))))
    case _ =>
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
      .setTradeDate(item(1).flatMap(_.asEPOC(df)).map(n => n: JLong).orNull)
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
