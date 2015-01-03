package com.shocktrade.actors

import java.text.SimpleDateFormat

import akka.actor.{Actor, ActorRef}
import com.ldaniels528.broadway.core.resources._
import com.ldaniels528.broadway.server.etl.actors.FileReadingActor._
import com.ldaniels528.trifecta.util.StringHelper._
import com.shocktrade.helpers.ConversionHelper._
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap

/**
 * EODData.com Enrichment Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class EodDataEnrichmentActor(target: ActorRef) extends Actor {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val sdf = new SimpleDateFormat("yyyyMMdd")
  private val processing = TrieMap[ReadableResource, Long]()

  override def receive = {
    case OpeningFile(resource) =>
      processing(resource) = System.currentTimeMillis()

    case ClosingFile(resource) =>
      processing.get(resource) foreach { startTime =>
        logger.info(s"Resource $resource completed in ${System.currentTimeMillis() - startTime} msecs")
        processing -= resource
      }

    case TextLine(resource, lineNo, line, tokens) =>
      // skip the header line
      if (lineNo != 1) {
        target ! toAvro(resource, tokens)
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
    val items = tokens map (_.trim) map (s => if (s.isEmpty) null else s)
    def item(index: Int) = if (index < items.length) items(index) else null

    val builder = com.shocktrade.avro.EodDataRecord.newBuilder()
    builder.setSymbol(item(0))
    builder.setExchange(resource.getResourceName.flatMap(extractExchange).orNull)
    builder.setTradeDate(item(1).asEPOC(sdf))
    builder.setOpen(item(2).asDouble)
    builder.setHigh(item(3).asDouble)
    builder.setLow(item(4).asDouble)
    builder.setClose(item(5).asDouble)
    builder.setVolume(item(6).asLong)
    builder.build()
  }

  private def extractExchange(name: String) = name.indexOptionOf("_") map (n => name.substring(0, n))

}
