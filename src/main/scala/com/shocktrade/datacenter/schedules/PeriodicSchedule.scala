package com.shocktrade.datacenter.schedules

import scala.concurrent.duration._
import com.ldaniels528.broadway.core.triggers.schedules.Scheduling

/**
 * Represents a periodically active schedule
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class PeriodicSchedule(id: String) extends Scheduling {
  private var lastCheckMillis: Long = 0L

  override def isEligible(eventTime: Long) = {
    val isReady = lastCheckMillis == 0 || eventTime - lastCheckMillis >= 10.minutes.toMillis
    lastCheckMillis = System.currentTimeMillis()
    isReady
  }

}
