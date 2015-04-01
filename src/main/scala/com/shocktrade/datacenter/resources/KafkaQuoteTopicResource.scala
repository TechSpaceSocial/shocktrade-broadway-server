package com.shocktrade.datacenter.resources

import com.ldaniels528.broadway.core.resources.Resource

/**
 * Kafka Quote Topic Resource
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class KafkaQuoteTopicResource(topic: String) extends Resource {

  /**
   * Returns an option of the resource name
   * @return an option of the resource name
   */
  override def getResourceName: Option[String] = Some(topic)

}
