package com.shocktrade.helpers

import java.lang.{Double => JDouble, Long => JLong}
import java.text.SimpleDateFormat

import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
 * Conversion Helper
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object ConversionHelper {
  private[this] lazy val logger = LoggerFactory.getLogger(getClass)

  /**
   * String conversion syntactic sugar
   * @param s the given string
   */
  implicit class StringConversion(val s: String) extends AnyVal {

    def asEPOC(sdf: SimpleDateFormat): JLong = {
      Try(sdf.parse(s)) match {
        case Success(date) => date.getTime
        case Failure(e) =>
          logger.error(s"Error parsing date string '$s': ${e.getMessage}")
          null
      }
    }

    def asDouble = {
      Try(s.toDouble) match {
        case Success(value) => value: JDouble
        case Failure(e) =>
          logger.error(s"Error parsing double string '$s': ${e.getMessage}")
          null
      }
    }

    def asLong = {
      Try(s.toLong) match {
        case Success(value) => value: JLong
        case Failure(e) =>
          logger.error(s"Error parsing long string '$s': ${e.getMessage}")
          null
      }
    }

  }

}
