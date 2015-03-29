package com.shocktrade.schedules

import com.ldaniels528.broadway.core.schedules.Scheduling
import com.shocktrade.services.util.DateUtil

/**
 * Represents a Trading Hours Schedule
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class TradingSchedule(id: String) extends Scheduling {
  private var lastRunMillis = System.currentTimeMillis()

  override def isEligible(eventTime: Long) = {
    val isReady = (eventTime - lastRunMillis >= 60000L) //&& DateUtil.isTradingActive(eventTime)
    if (isReady) lastRunMillis = System.currentTimeMillis()
    isReady
  }

}
