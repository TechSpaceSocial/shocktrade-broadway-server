package com.shocktrade.datacenter.actors.yahoo

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor.{Publish, PublishAvro}
import com.ldaniels528.trifecta.io.avro.AvroConversion
import com.shocktrade.datacenter.actors.yahoo.QuoteLookupAndPublishActor.parameters
import com.shocktrade.services.{YFStockQuoteService, YahooFinanceServices}

/**
 * Stock Quote Lookup and Publish Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class QuoteLookupAndPublishActor(topic: String, target: ActorRef) extends Actor with ActorLogging {

  import context.dispatcher

  override def receive = {
    case symbol: String => transmit(symbol)
    case symbols: Array[String] => transmit(symbols)
    case message =>
      log.error(s"Unhandled message $message")
      unhandled(message)
  }

  private def transmit(symbol: String) {
    YFStockQuoteService.getCSVData(Seq(symbol), parameters) foreach {
      _ foreach { line =>
        target ! Publish(topic, message = s"$symbol|$parameters|$line".getBytes("UTF8"))
      }
    }
  }

  private def transmit(symbols: Array[String]) {
    YFStockQuoteService.getCSVData(symbols, parameters) foreach { lines =>
      (symbols zip lines.toSeq) foreach { case (symbol, line) =>
        target ! Publish(topic, message = s"$symbol|$parameters|$line".getBytes("UTF8"))
      }
    }
  }

  private def transmitAsAvro(symbol: String) {
    YahooFinanceServices.getStockQuote(symbol, parameters) foreach { quote =>
      val builder = com.shocktrade.avro.CSVQuoteRecord.newBuilder()
      AvroConversion.copy(quote, builder)
      target ! PublishAvro(topic, record = builder.build())
    }
  }

  private def transmitAsAvro(symbols: Seq[String]) {
    YahooFinanceServices.getStockQuotes(symbols, parameters) foreach { quotes =>
      quotes foreach { quote =>
        val builder = com.shocktrade.avro.CSVQuoteRecord.newBuilder()
        AvroConversion.copy(quote, builder)
        target ! PublishAvro(topic, record = builder.build())
      }
    }
  }

}

/**
 * Stock Quote Lookup and Publish Actor Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object QuoteLookupAndPublishActor {
  val parameters = YFStockQuoteService.getParams(
    "symbol", "exchange", "lastTrade", "tradeDate", "tradeTime", "change", "changePct", "prevClose", "open", "close",
    "high", "low", "high52Week", "low52Week", "volume", "marketCap", "errorMessage", "ask", "askSize", "bid", "bidSize")

}