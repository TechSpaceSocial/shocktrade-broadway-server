package com.shocktrade.datacenter.schedules

import com.ldaniels528.broadway.core.triggers.schedules.Scheduling
import com.shocktrade.services.util.DateUtil

/**
 * Represents a Trading Hours Schedule
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class USStockMarketSchedule(id: String) extends Scheduling {
  private var lastCheckMillis: Long = 0

  override def isEligible(eventTime: Long) = {
    val isReady = (eventTime - lastCheckMillis >= 60000L) && DateUtil.isTradingActive(eventTime)
    lastCheckMillis = System.currentTimeMillis()
    isReady
  }

}
