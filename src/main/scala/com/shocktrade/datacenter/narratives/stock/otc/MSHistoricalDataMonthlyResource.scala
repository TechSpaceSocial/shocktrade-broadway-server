package com.shocktrade.datacenter.narratives.stock.otc

import java.net.{URL, URLEncoder}
import java.text.SimpleDateFormat

import com.ldaniels528.broadway.core.resources.ReadableResource
import org.joda.time.DateTime

/**
 * OTCE Finra MS Historical Data Monthly Resource
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 * @see http://otce.finra.org/MSHistoricalDataMonthly/Archives
 */
class MSHistoricalDataMonthlyResource() extends ReadableResource {
  private var lastDate = formatDate(new DateTime().minusDays(1))

  /*
   * month|year|domestic_issues|foreign_issues|ADR_issues|total_share_volume|total_dollar_volume|transactions|trading_days|securities|ad_domestic_share_volume|ad_domestic_dollar_volume|ad_domestic_issues_traded|ad_foreign_issues_traded|ad_foreign_share_volume|ad_foreign_dollar_volume|ad_ADRs_traded|ad_ADRs_share_volume|ad_ADRs_dollar_volume
   * 3|2015|7718|7998|2560|142528538051|19614649581|2301744|22|18277|140383561568|4129380245|5447|4406|1422598323|3084731302|1236|722378160|12400538035
   */

  /**
   * Returns an option of an input stream to the resource
   * @return an option of an input stream
   */
  override def getInputStream = {
    // http://otce.finra.org/MSHistoricalDataMonthly/DownloadFileStream?fileLocation=D%3A\OTCE\DownloadFiles\HistoricalData\OtherOTC_032015.txt
    getResourceName map (f =>
      new URL(URLEncoder.encode(s"http://otce.finra.org/MSHistoricalDataMonthly/DownloadFileStream?fileLocation=$f", "UTF8")).openStream())
  }

  /**
   * Returns an option of the resource name
   * @return an option of the resource name
   */
  override def getResourceName = {
    val today = formatDate(new DateTime())
    if (today != lastDate) {
      lastDate = today
      Some(s"D:\\OTCE\\DownloadFiles\\HistoricalData\\OtherOTC_$today.txt")
    }
    else None
  }

  private def formatDate(date: DateTime) = new SimpleDateFormat("MMyyyy").format(date.toDate)

}
