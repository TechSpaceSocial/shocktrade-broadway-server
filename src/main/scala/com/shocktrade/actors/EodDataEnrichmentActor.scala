package com.shocktrade.actors

import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.{Actor, ActorRef}
import com.ldaniels528.broadway.server.etl.actors.FileReadingActor.EOF
import com.ldaniels528.trifecta.io.avro.AvroConversion
import com.shocktrade.actors.EodDataEnrichmentActor.EODHistoricalQuote
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
 * EODData.com Enrichment Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class EodDataEnrichmentActor(target: ActorRef) extends Actor {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private implicit val sdf = new SimpleDateFormat("yyyyMMdd")

  override def receive = {
    case EOF(resource) =>
      logger.info(s"Resource $resource completed")
    case sections: Array[String] =>
      val quote = toHistoricalQuote(sections)
      val builder = com.shocktrade.avro.EodDataRecord.newBuilder()
      AvroConversion.copy(quote, builder)
      target ! builder.build()
    case message =>
      unhandled(message)
  }

  /**
   * Transforms the given line into a historical quote
   */
  private def toHistoricalQuote(sections: Array[String])(implicit sdf: SimpleDateFormat) = {
    val items = sections map (_.trim) map (s => if (s.length == 0) None else Some(s))
    def item(index: Int) = if (items.length > index) items(index) else None

    EODHistoricalQuote(
      item(0),
      item(1) flatMap parseDate,
      item(2) map (_.toDouble),
      item(3) map (_.toDouble),
      item(4) map (_.toDouble),
      item(5) map (_.toDouble),
      item(6) map (_.toLong))
  }

  private def parseDate(dateString: String)(implicit sdf: SimpleDateFormat): Option[Date] = {
    Try(sdf.parse(dateString)) match {
      case Success(date) => Some(date)
      case Failure(e) =>
        logger.error(s"Failed to parse date '$dateString' (yyyyMMdd)")
        None
    }
  }

}

/**
 * EODData.com Enrichment Actor Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object EodDataEnrichmentActor {

  /**
   * Represents an End-Of-Day Historical Quote
   */
  case class EODHistoricalQuote(symbol: Option[String],
                                tradeDate: Option[Date],
                                open: Option[Double],
                                high: Option[Double],
                                low: Option[Double],
                                close: Option[Double],
                                volume: Option[Long])

}