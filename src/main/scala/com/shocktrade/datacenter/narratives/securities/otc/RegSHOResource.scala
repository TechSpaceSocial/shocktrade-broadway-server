package com.shocktrade.datacenter.narratives.securities.otc

import java.net.{URL, URLEncoder}
import java.text.SimpleDateFormat

import com.ldaniels528.broadway.core.resources.ReadableResource
import org.joda.time.DateTime

/**
 * OTCE Finra Reg SHO Resource
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 * @see http://otce.finra.org/RegSHO/Archives
 */
class RegSHOResource(id: String) extends ReadableResource {
  private var lastDate = formatDate(new DateTime().minusDays(1))

  /*
   * Symbol|Security Name|Market Category|Reg SHO Threshold Flag|Rule 4320
   * DROP|Fuse Science, Inc. Common Stock|u|Y|N
   * DTST|Data Storage Corporation Common Stock|u|Y|N
   * ENIP|Endeavor IP, Inc. Common Stock|U|Y|N
   * JALA|Be Active Holdings, Inc. Common Stock|u|Y|N
   * AVTO|Avantogen Oncology, Inc. Common Stock|u|Y|N
   */

  /**
   * Returns an option of an input stream to the resource
   * @return an option of an input stream
   */
  override def getInputStream = {
    // http://otce.finra.org/RegSHO/DownloadFileStream?fileLocation=D:\OTCE\DownloadFiles\SHO\otc-thresh20150410_201504102300.txt
    getResourceName map (f =>
      new URL(URLEncoder.encode(s"http://otce.finra.org/RegSHO/DownloadFileStream?fileLocation=$f", "UTF8")).openStream())
  }

  /**
   * Returns an option of the resource name
   * @return an option of the resource name
   */
  override def getResourceName = {
    val today = formatDate(new DateTime())
    if (today != lastDate) {
      lastDate = today
      Some(s"D:\\OTCE\\DownloadFiles\\SHO\\otc-thresh${today}_${today}2300.txt")
    }
    else None
  }

  private def formatDate(date: DateTime) = new SimpleDateFormat("yyyyMMdd").format(date.toDate)

}
