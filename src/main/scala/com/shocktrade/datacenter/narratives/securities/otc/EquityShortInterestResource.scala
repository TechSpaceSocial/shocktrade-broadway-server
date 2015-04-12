package com.shocktrade.datacenter.narratives.securities.otc

import java.net.{URL, URLEncoder}
import java.text.SimpleDateFormat

import com.ldaniels528.broadway.core.resources.ReadableResource
import org.joda.time.DateTime

/**
 * OTCE Finra Equity Short Interest (ESI) Resource
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 * @see http://otce.finra.org/ESI/Archives
 */
class EquityShortInterestResource() extends ReadableResource {
  private var lastDate = formatDate(new DateTime().minusDays(1))

  /*
   * Security Name|Security Symbol|OTC Market|Current Shares Short|Previous Report Shares Short|Change in Shares Short from Previous Report|Percent Change from Previous Report|Average Daily Share Volume|Days to Cover
   * Aareal Bank AG AKT|AAALF|u|7409|131879|-124470|-94.38|153|48.42
   * Aareal Bank AG Unsponsored Ame|AAALY|u|200|200|0|0|8|25
   * Altin Ag Baar Namen-AKT|AABNF|u|20|20|0|0|0|999.99
   * AAC Technologies Holdings Inc|AACAF|u|14660946|13732643|928303|6.7
   */

  /**
   * Returns an option of an input stream to the resource
   * @return an option of an input stream
   */
  override def getInputStream = {
    // http://otce.finra.org/ESI/DownloadFileStream?fileLocation=D%3A\OTCE\DownloadFiles\ESI\shrt20153103.txt
    getResourceName map (f =>
      new URL(URLEncoder.encode(s"http://otce.finra.org/ESI/DownloadFileStream?fileLocation=$f", "UTF8")).openStream())
  }

  /**
   * Returns an option of the resource name
   * @return an option of the resource name
   */
  override def getResourceName = {
    val today = formatDate(new DateTime())
    if (today != lastDate) {
      lastDate = today
      Some(s"D:\\OTCE\\DownloadFiles\\ESI\\shrt$today.txt")
    }
    else None
  }

  private def formatDate(date: DateTime) = new SimpleDateFormat("yyyyddMM").format(date.toDate)

}
