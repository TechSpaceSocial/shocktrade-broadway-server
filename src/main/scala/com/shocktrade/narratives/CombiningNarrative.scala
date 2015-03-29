package com.shocktrade.narratives

import akka.actor.Actor
import com.ldaniels528.broadway.BroadwayNarrative
import com.ldaniels528.broadway.core.actors.FileReadingActor
import com.ldaniels528.broadway.core.actors.FileReadingActor.{BinaryBlock, CopyText, TextLine}
import com.ldaniels528.broadway.core.resources.RandomAccessFileResource
import com.ldaniels528.broadway.server.ServerConfig
import com.shocktrade.narratives.CombiningNarrative.FileWritingActor

/**
 * File Combining Narrative
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class CombiningNarrative(config: ServerConfig) extends BroadwayNarrative(config, "File Combining") {
  // create a file reader actor to read lines from the incoming resource
  val fileReader = addActor(new FileReadingActor(config))

  // create an actor to copy the contents to
  val fileWriter = addActor(new FileWritingActor(config, RandomAccessFileResource("/Users/ldaniels/NASDAQ-bundle.txt")))

  onStart { resource =>
    // start the processing by submitting a request to the file reader actor
    fileReader ! CopyText(resource, fileWriter)
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
