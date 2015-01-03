package com.shocktrade.actors

import java.text.SimpleDateFormat

import akka.actor.{Actor, ActorRef}
import com.ldaniels528.broadway.core.resources._
import com.ldaniels528.broadway.server.etl.actors.FileReadingActor._
import com.ldaniels528.trifecta.util.StringHelper._
import com.shocktrade.helpers.ConversionHelper._
import com.shocktrade.helpers.ResourceTracker

/**
 * EODData.com Enrichment Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class EodDataEnrichmentActor(target: ActorRef) extends Actor {
  private val sdf = new SimpleDateFormat("yyyyMMdd")

  override def receive = {
    case OpeningFile(resource) =>
      ResourceTracker.start(resource)

    case ClosingFile(resource) =>
      ResourceTracker.stop(resource)

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

  /**
   * Extracts the stock exchange from the given file name
   * @param name the given file name (e.g. "NASDAQ_20120206.txt")
   * @return an option of the stock exchange (e.g. "NASDAQ")
   */
  private def extractExchange(name: String) = name.indexOptionOf("_") map (n => name.substring(0, n))

}
