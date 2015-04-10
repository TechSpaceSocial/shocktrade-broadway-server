package com.shocktrade.datacenter.narratives.stock

import java.lang.{Double => JDouble}

import akka.actor.ActorRef
import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor.Find
import com.mongodb.casbah.Imports.{DBObject => O, _}
import org.joda.time.DateTime

/**
 * Provide commonly used functions for computing stock quote-related values
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait StockQuoteSupport {
  self: BroadwayNarrative =>

  def computeSpread(high: Option[JDouble], low: Option[JDouble]) = {
    for {
      hi <- high
      lo <- low
    } yield if (lo != 0.0d) 100d * (hi - lo) / lo else 0.0d
  }

  def computeChangePct(prevClose: Option[JDouble], lastTrade: Option[JDouble]): Option[JDouble] = {
    for {
      prev <- prevClose
      last <- lastTrade
      diff = last - prev
    } yield (if (diff != 0) 100d * (diff / prev) else 0.0d): JDouble
  }

  def symbolLookupQuery(recipient: ActorRef, collectionName: String, lastModified: DateTime, fetchSize: Int = 64) = {
    log.info(s"Retrieving symbols from collection $collectionName (modified since $lastModified)...")
    Find(
      recipient,
      name = collectionName,
      query = O("active" -> true, "yfDynUpdates" -> true) ++ $or("yfDynLastUpdated" $exists false, "yfDynLastUpdated" $lte lastModified),
      fields = O("symbol" -> 1),
      maxFetchSize = fetchSize)
  }

}
