package com.shocktrade.datacenter.schedules

import com.ldaniels528.broadway.core.triggers.schedules.Scheduling

/**
 * Represents a periodically active schedule
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class PeriodicSchedule(id: String) extends Scheduling {
  private var lastCheckMillis: Long = 0L

  override def isEligible(eventTime: Long) = {
    val isReady = lastCheckMillis == 0 || eventTime - lastCheckMillis >= 300000L
    lastCheckMillis = System.currentTimeMillis()
    isReady
  }

}
