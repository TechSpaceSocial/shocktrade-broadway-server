package com.shocktrade.datacenter.narratives

import java.util.Properties

import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.util.PropertiesHelper._
import com.ldaniels528.broadway.server.ServerConfig
import com.ldaniels528.trifecta.io.zookeeper.ZKProxy
import com.shocktrade.datacenter.actors.{ConsumerResetActor, ConsumerScanningActor, ScanCoordinatingActor}

/**
 * Consumer Scan Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ConsumerScanNarrative(config: ServerConfig, id: String, props: Properties)
  extends BroadwayNarrative(config, id, props) {

  implicit val zk = ZKProxy(props.getOrDie("zookeeper.connect"))

  // create the consumer group scanning actor
  lazy val scanActor = prepareActor(new ConsumerScanningActor())

  // create the consumer group reset actor
  lazy val resetActor = prepareActor(new ConsumerResetActor(scanActor))

  // create the partition coordinating actor
  lazy val coordinator = prepareActor(new ScanCoordinatingActor(resetActor))

  onStart { resource =>
    // start the processing by submitting a request to the file reader actor
    coordinator ! "dev"
  }
}
