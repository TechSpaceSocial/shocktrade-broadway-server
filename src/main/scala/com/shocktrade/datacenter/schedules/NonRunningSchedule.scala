package com.shocktrade.datacenter.schedules

import com.ldaniels528.broadway.core.schedules.Scheduling

/**
 * Represents a non-running schedule
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class NonRunningSchedule(id: String) extends Scheduling {

  override def isEligible(eventTime: Long) = false
}
