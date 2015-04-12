package com.shocktrade.datacenter.narratives.securities.otc

import java.net.URL
import java.text.SimpleDateFormat
import java.util.Date

import com.ldaniels528.broadway.core.resources.ReadableResource

/**
 * OTC Bulletin Board Daily List Resource
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
@deprecated
class OTCBBDailyListResource() extends ReadableResource {

  /**
   * Returns an option of an input stream to the resource (e.g. "http://www.otcbb.com/dailylist/txthistory/BB01202015.txt")
   * @return an option of an input stream
   */
  override def getInputStream = {
    getResourceName map (f => new URL(s"http://www.otcbb.com/dailylist/txthistory/$f").openStream())
  }

  /**
   * Returns an option of the resource name
   * @return an option of the resource name
   */
  override def getResourceName = Some(s"BB${new SimpleDateFormat("MMddyyyy").format(new Date())}.txt")

}
