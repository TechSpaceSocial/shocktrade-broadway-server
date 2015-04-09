package com.shocktrade.datacenter.narratives.stock.yahoo

import java.lang.{Double => JDouble, Long => JLong}
import java.util.Properties

import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.TransformingActor
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor
import com.ldaniels528.broadway.core.actors.kafka.KafkaPublishingActor.PublishAvro
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor._
import com.ldaniels528.broadway.core.util.PropertiesHelper._
import com.ldaniels528.broadway.server.ServerConfig
import com.mongodb.casbah.Imports._
import com.shocktrade.avro.KeyStatisticsRecord
import com.shocktrade.datacenter.narratives.stock.SymbolQuerying
import com.shocktrade.services.YFKeyStatisticsService
import com.shocktrade.services.YFKeyStatisticsService.YFKeyStatistics
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

/**
 * Yahoo! Finance Key Statistics Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class YahooKeyStatisticsNarrative(config: ServerConfig, id: String, props: Properties)
  extends BroadwayNarrative(config, id, props)
  with SymbolQuerying {
  lazy val log = LoggerFactory.getLogger(getClass)

  // extract the properties we need
  val kafkaTopic = props.getOrDie("kafka.topic")
  val mongoReplicas = props.getOrDie("mongo.replicas")
  val mongoDatabase = props.getOrDie("mongo.database")
  val mongoCollection = props.getOrDie("mongo.collection")
  val zkConnect = props.getOrDie("zookeeper.connect")

  // create a MongoDB actor for retrieving stock quotes
  lazy val mongoReader = prepareActor(MongoDBActor(parseServerList(mongoReplicas), mongoDatabase), parallelism = 1)

  // create a Kafka publishing actor
  lazy val kafkaPublisher = prepareActor(new KafkaPublishingActor(zkConnect), parallelism = 1)

  // create an actor to transform the MongoDB results to Avro-encoded records
  lazy val transformer = prepareActor(new TransformingActor({
    case MongoFindResults(coll, docs) =>
      docs.flatMap(_.getAs[String]("symbol")) foreach { symbol =>
        kafkaPublisher ! PublishAvro(kafkaTopic, toAvro(YFKeyStatisticsService.getKeyStatisticsSync(symbol)))
      }
      true
    case _ => false
  }))

  onStart { resource =>
    // Sends the symbols to the transforming actor, which will load the quote, transform it to Avro,
    // and send it to Kafka
    mongoReader ! symbolLookupQuery(transformer, mongoCollection, new DateTime().minusMinutes(5))
  }

  private def toAvro(ks: YFKeyStatistics) = {
    KeyStatisticsRecord.newBuilder()
      .setSymbol(ks.symbol)
      .setPctHeldByInsiders(ks.pctHeldByInsiders.map(n => n: JDouble).orNull)
      .setPctHeldByInstitutions(ks.pctHeldByInstitutions.map(n => n: JDouble).orNull)
      .setDividendYield5YearAvg(ks.dividendYield5YearAvg.map(n => n: JDouble).orNull)
      .setChange52Week(ks.change52Week.map(n => n: JDouble).orNull)
      .setHigh52Week(ks.high52Week.map(n => n: JDouble).orNull)
      .setLow52Week(ks.low52Week.map(n => n: JDouble).orNull)
      .setMovingAverage50Day(ks.movingAverage50Day.map(n => n: JDouble).orNull)
      .setMovingAverage200Day(ks.movingAverage200Day.map(n => n: JDouble).orNull)
      .setAvgVolume3Month(ks.avgVolume3Month.map(n => n: JLong).orNull)
      .setAvgVolume10Day(ks.avgVolume10Day.map(n => n: JLong).orNull)
      .setBeta(ks.beta.map(n => n: JDouble).orNull)
      .setBookValuePerShare(ks.bookValuePerShare.map(n => n: JDouble).orNull)
      .setCurrentRatio(ks.currentRatio.map(n => n: JDouble).orNull)
      .setDilutedEPS(ks.dilutedEPS.map(n => n: JDouble).orNull)
      .setDividendDate(ks.dividendDate.map(n => n.getTime: JLong).orNull)
      .setEBITDA(ks.EBITDA.map(n => n: JDouble).orNull)
      .setEnterpriseValue(ks.enterpriseValue.map(n => n: JDouble).orNull)
      .setEnterpriseValueOverEBITDA(ks.enterpriseValueOverEBITDA.map(n => n: JDouble).orNull)
      .setEnterpriseValueOverRevenue(ks.enterpriseValueOverRevenue.map(n => n: JDouble).orNull)
      .setExDividendDate(ks.exDividendDate.map(n => n.getTime: JLong).orNull)
      .setFiscalYearEndDate(ks.fiscalYearEndDate.map(n => n.getTime: JLong).orNull)
      .setSharesFloat(ks.sharesFloat.map(n => n: JLong).orNull)
      .setForwardAnnualDividendRate(ks.forwardAnnualDividendRate.map(n => n: JDouble).orNull)
      .setForwardAnnualDividendYield(ks.forwardAnnualDividendYield.map(n => n: JDouble).orNull)
      .setForwardPE(ks.forwardPE.map(n => n: JDouble).orNull)
      .setGrossProfit(ks.grossProfit.map(n => n: JDouble).orNull)
      .setLastSplitDate(ks.lastSplitDate.map(n => n.getTime: JLong).orNull)
      .setLastSplitFactor(ks.lastSplitFactor.orNull)
      .setLeveredFreeCashFlow(ks.leveredFreeCashFlow.map(n => n: JDouble).orNull)
      .setMarketCapIntraday(ks.marketCapIntraday.map(n => n: JDouble).orNull)
      .setMostRecentQuarterDate(ks.mostRecentQuarterDate.map(n => n.getTime: JLong).orNull)
      .setNetIncomeAvailToCommon(ks.netIncomeAvailToCommon.map(n => n: JDouble).orNull)
      .setOperatingCashFlow(ks.operatingCashFlow.map(n => n: JDouble).orNull)
      .setOperatingMargin(ks.operatingMargin.map(n => n: JDouble).orNull)
      .setPegRatio5YearExp(ks.pegRatio5YearExp.map(n => n: JDouble).orNull)
      .setPayoutRatio(ks.payoutRatio.map(n => n: JDouble).orNull)
      .setPriceOverBookValue(ks.priceOverBookValue.map(n => n: JDouble).orNull)
      .setPriceOverSales(ks.priceOverSales.map(n => n: JDouble).orNull)
      .setProfitMargin(ks.profitMargin.map(n => n: JDouble).orNull)
      .setEarningsGrowthQuarterly(ks.earningsGrowthQuarterly.map(n => n: JDouble).orNull)
      .setRevenueGrowthQuarterly(ks.revenueGrowthQuarterly.map(n => n: JDouble).orNull)
      .setReturnOnAssets(ks.returnOnAssets.map(n => n: JDouble).orNull)
      .setReturnOnEquity(ks.returnOnEquity.map(n => n: JDouble).orNull)
      .setRevenue(ks.revenue.map(n => n: JDouble).orNull)
      .setRevenuePerShare(ks.revenuePerShare.map(n => n: JDouble).orNull)
      .setChange52WeekSNP500(ks.change52WeekSNP500.map(n => n: JDouble).orNull)
      .setSharesOutstanding(ks.sharesOutstanding.map(n => n: JLong).orNull)
      .setSharesShort(ks.sharesShort.map(n => n: JLong).orNull)
      .setSharesShortPriorMonth(ks.sharesShortPriorMonth.map(n => n: JLong).orNull)
      .setShortPctOfFloat(ks.shortPctOfFloat.map(n => n: JDouble).orNull)
      .setShortRatio(ks.shortRatio.map(n => n: JDouble).orNull)
      .setTotalCash(ks.totalCash.map(n => n: JDouble).orNull)
      .setTotalCashPerShare(ks.totalCashPerShare.map(n => n: JDouble).orNull)
      .setTotalDebt(ks.totalDebt.map(n => n: JDouble).orNull)
      .setTotalDebtOverEquity(ks.totalDebtOverEquity.map(n => n: JDouble).orNull)
      .setTrailingAnnualDividendYield(ks.trailingAnnualDividendYield.map(n => n: JDouble).orNull)
      .setTrailingPE(ks.trailingPE.map(n => n: JDouble).orNull)
      .setResponseTimeMsec(ks.responseTimeMsec: JLong)
      .build()
  }

}
