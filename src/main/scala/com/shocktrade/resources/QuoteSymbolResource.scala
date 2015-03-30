package com.shocktrade.resources

import com.ldaniels528.broadway.core.resources.IterableResource
import com.mongodb.ServerAddress
import com.mongodb.casbah.Imports.{DBObject => O, _}
import com.mongodb.casbah.commons.conversions.scala.RegisterJodaTimeConversionHelpers
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
 * Quote Symbol Resource
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class QuoteSymbolResource(name: String) extends IterableResource[String] {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private lazy val mconn = getConnection(List(
    new ServerAddress("dev528", 27017),
    new ServerAddress("dev601", 27017),
    new ServerAddress("dev602", 27017),
    new ServerAddress("dev603", 27017)
  ))
  private lazy implicit val mcoll = mconn("shocktrade")("Stocks")

  // register the time/date helpers
  RegisterJodaTimeConversionHelpers()

  /**
   * Returns an option of the resource name
   * @return an option of the resource name
   */
  override def getResourceName: Option[String] = Some(name)

  /**
   * Retrieves the qualifying symbols
   * db.Stocks.find({symbol: {$in : ["AMD", "AAPL", "MSFT"]}}, {symbol:1, exchange:1, yfRealTimeUpdates:1, yfDynUpdates:1})
   * db.Stocks.update({symbol:{$in : ["AMD", "AAPL", "MSFT"]}}, {$set:{"yfRealTimeUpdates":true, "yfDynUpdates":false}}, {multi:true})
   */
  override def iterator: Iterator[String] = {
    val startTime = System.nanoTime()
    Try {
      logger.info(s"Retrieving symbols for CSV quote updates...")
      val _5_min_ago = new DateTime().minusMinutes(5)
      mcoll.find(
        O("active" -> true, "yfDynUpdates" -> true) ++ $or("yfDynLastUpdated" $exists false, "yfDynLastUpdated" $lte _5_min_ago),
        O("symbol" -> 1)) flatMap (_.getAs[String]("symbol"))
    } match {
      case Success(results) =>
        val elapsedTime = (System.nanoTime() - startTime) / 1e+6
        logger.info(f"Retrieved symbols for CSV quote updates [$elapsedTime%.1f msecs]")
        results
      case Failure(e) =>
        logger.error("Failed to retrieve symbols from database", e)
        throw new IllegalStateException(e)
    }
  }

  override def toString = s"${getClass.getName}($name)"

  /**
   * Creates a new database connection
   */
  def getConnection(replicaSets: List[ServerAddress]): MongoConnection = {
    // create the client options
    val clientOptions = MongoClientOptions(
      autoConnectRetry = true,
      connectionsPerHost = 100,
      maxAutoConnectRetryTime = 3,
      maxWaitTime = 5000,
      socketKeepAlive = true,
      socketTimeout = 6000,
      threadsAllowedToBlockForConnectionMultiplier = 50,
      writeConcern = WriteConcern.Safe)

    // create the connection
    MongoConnection(replicaSets, new MongoOptions(clientOptions))
  }

}
