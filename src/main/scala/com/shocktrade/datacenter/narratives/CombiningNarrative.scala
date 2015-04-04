package com.shocktrade.datacenter.narratives

import java.util.Properties

import akka.actor.Actor
import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.FileReadingActor
import com.ldaniels528.broadway.core.actors.FileReadingActor.{BinaryBlock, CopyText, TextLine}
import com.ldaniels528.broadway.core.resources.{RandomAccessFileResource, ReadableResource}
import com.ldaniels528.broadway.server.ServerConfig
import com.shocktrade.datacenter.narratives.CombiningNarrative.FileWritingActor

/**
 * File Combining Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class CombiningNarrative(config: ServerConfig, id: String, props: Properties)
  extends BroadwayNarrative(config, id, props) {

  // create a file reader actor to read lines from the incoming resource
  lazy val fileReader = prepareActor(new FileReadingActor(config))

  // create an actor to copy the contents to
  lazy val fileWriter = prepareActor(new FileWritingActor(config, RandomAccessFileResource("/Users/ldaniels/NASDAQ-bundle.txt")))

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

/**
 * File Combining Narrative Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object CombiningNarrative {

  class FileWritingActor(config: ServerConfig, output: RandomAccessFileResource) extends Actor {
    override def receive = {
      case BinaryBlock(resource, offset, bytes) =>
        output.write(offset, bytes)
      case TextLine(resource, lineNo, line, tokens) =>
        if (lineNo > 1) output.write(line)
      case message =>
        unhandled(message)
    }
  }

}
