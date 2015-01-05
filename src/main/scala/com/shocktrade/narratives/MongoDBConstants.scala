package com.shocktrade.narratives

import com.mongodb.casbah.Imports._

/**
 * MongoD BConstants
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait MongoDBConstants {

  // define the connection parameters
  val ShockTradeDB = "shocktrade"

  val StockQuotes = "Stocks"

  val MongoDBServers = List(
    new ServerAddress("dev601", 27017),
    new ServerAddress("dev602", 27017),
    new ServerAddress("dev603", 27017)
  )

}
