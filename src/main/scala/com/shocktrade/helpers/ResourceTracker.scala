package com.shocktrade.helpers

import com.ldaniels528.broadway.core.resources.ReadableResource
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap

/**
 * Resource Tracker
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object ResourceTracker {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val processing = TrieMap[ReadableResource, Long]()

  def start(resource: ReadableResource) {
    processing(resource) = System.currentTimeMillis()
  }

  def stop(resource: ReadableResource) = {
    processing.get(resource) foreach { startTime =>
      logger.info(s"Resource $resource completed in ${System.currentTimeMillis() - startTime} msecs")
      processing -= resource
    }
  }

}
