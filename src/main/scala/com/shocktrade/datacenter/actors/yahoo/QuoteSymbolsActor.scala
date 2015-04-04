package com.shocktrade.datacenter.actors.yahoo

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor.{Find, MongoResult}
import com.mongodb.casbah.Imports.{DBObject => O, _}
import com.shocktrade.datacenter.actors.yahoo.CsvQuotesYahooToKafkaNarrative.RequestQuotes
import org.joda.time.DateTime

/**
 * Stock Quote Symbol Retrieval Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class QuoteSymbolsActor(mongoReader: ActorRef, mongoCollection: String, quoteLookup: ActorRef) extends Actor with ActorLogging {
  override def receive = {
    // when a receive the request quotes message, I shall fire a find message to the MongoDB actor
    case RequestQuotes =>
      val _5_mins_ago = new DateTime().minusMinutes(5)
      mongoReader ! Find(
        name = mongoCollection,
        query = O("active" -> true, "yfDynUpdates" -> true) ++ $or("yfDynLastUpdated" $exists false, "yfDynLastUpdated" $lte _5_mins_ago),
        fields = O("symbol" -> 1)
      )

    // .. and I shall forward all responses to the quote lookup actor
    case MongoResult(doc) => doc.getAs[String]("symbol") foreach (quoteLookup ! _)

    case message => unhandled(message)
  }
}

/**
 * Stock Quote Symbol Retrieval Actor Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object CsvQuotesYahooToKafkaNarrative {

  case object RequestQuotes

}
