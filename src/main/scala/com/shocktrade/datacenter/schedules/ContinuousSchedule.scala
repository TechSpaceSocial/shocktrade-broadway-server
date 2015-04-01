package com.shocktrade.datacenter.schedules

import com.ldaniels528.broadway.core.schedules.Scheduling

/**
 * Represents a continuously active schedule
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class ContinuousSchedule(id: String) extends Scheduling {
  private var lastCheckMillis: Long = 0

  override def isEligible(eventTime: Long) = {
    val isReady = eventTime - lastCheckMillis >= 60000L
    lastCheckMillis = System.currentTimeMillis()
    isReady
  }

}
