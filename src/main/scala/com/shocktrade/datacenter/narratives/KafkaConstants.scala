package com.shocktrade.datacenter.narratives

/**
 * Kafka Topic Constants
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait KafkaConstants {
  val eodDataTopic = "eoddata.transhistory.avro"
  val keyStatsTopic = "yahoo.keystats.avro"
  val quotesTopic = "yahoo.quotes.avro"
  val otcTranHistoryTopic = "otcbb.transactions.avro"

  val zkHost = "dev501:2181"

}
