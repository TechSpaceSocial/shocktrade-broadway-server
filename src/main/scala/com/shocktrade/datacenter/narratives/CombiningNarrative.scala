package com.shocktrade.datacenter.narratives

import java.util.Properties

import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.file.FileReadingActor
import FileReadingActor.CopyText
import com.ldaniels528.broadway.core.actors.file.FileReadingActor
import com.ldaniels528.broadway.core.resources.{RandomAccessFileResource, ReadableResource}
import com.ldaniels528.broadway.server.ServerConfig
import com.shocktrade.datacenter.actors.FileWritingActor

/**
 * File Combining Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class CombiningNarrative(config: ServerConfig, id: String, props: Properties)
  extends BroadwayNarrative(config, id, props) {

  // create a file reader actor to read lines from the incoming resource
  lazy val fileReader = prepareActor(new FileReadingActor(config))

  // create an actor to copy the contents to
  lazy val fileWriter = prepareActor(new FileWritingActor(RandomAccessFileResource("/Users/ldaniels/NASDAQ-bundle.txt")))

  onStart {
    _ foreach {
      case resource: ReadableResource =>
        // start the processing by submitting a request to the file reader actor
        fileReader ! CopyText(resource, fileWriter)
      case _ =>
        throw new IllegalStateException(s"A ${classOf[ReadableResource].getName} was expected")
    }
  }
}
