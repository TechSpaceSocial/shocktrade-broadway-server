package com.shocktrade.datacenter.narratives.stock

import akka.actor.ActorRef
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor.Find
import com.mongodb.casbah.Imports.{DBObject => O, _}
import org.joda.time.DateTime

/**
 * Stock Symbol Querying Capability
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait SymbolQuerying {

  def symbolLookupQuery(recipient: ActorRef, collectionName: String, lastModified: DateTime) = {
    Find(
      recipient,
      name = collectionName,
      query = O("active" -> true, "yfDynUpdates" -> true) ++ $or("yfDynLastUpdated" $exists false, "yfDynLastUpdated" $lte lastModified),
      fields = O("symbol" -> 1))
  }

}
