package com.ldaniels528.broadway.server.http

import akka.actor.{Actor, ActorLogging, ActorRef, Props, _}
import akka.io.IO
import akka.io.Tcp.Bound
import akka.pattern.ask
import akka.util.Timeout
import org.slf4j.LoggerFactory
import spray.can.Http
import spray.http._

import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * Broadway Http Server
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class BroadwayHttpServer(host: String, port: Int)(implicit system: ActorSystem) {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val listenerActor = system.actorOf(Props[ClientHandlingActor], "clientHandler")

  import system.dispatcher

  def start(): Unit = {
    // start the HTTP server
    implicit val timeout: Timeout = 5 seconds
    val response = IO(Http) ? Http.Bind(listenerActor, interface = host, port = port)
    response.foreach {
      case Bound(interface) =>
        logger.info(s"Server is now bound to $interface")
      case outcome =>
        logger.error(s"Unexpected response $outcome")
    }
  }

}

/**
 * Broadway Http Server Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object BroadwayHttpServer {
  private[this] lazy val logger = LoggerFactory.getLogger(getClass)

  class StreamingActor(client: ActorRef, count: Int) extends Actor with ActorLogging {

    import context.dispatcher

    log.debug("Starting streaming response ...")

    // we use the successful sending of a chunk as trigger for scheduling the next chunk
    client ! ChunkedResponseStart(HttpResponse(entity = " " * 2048)).withAck(Ok(count))

    def receive = {
      case Ok(0) =>
        log.info("Finalizing response stream ...")
        client ! MessageChunk("\nStopped...")
        client ! ChunkedMessageEnd
        context.stop(self)

      case Ok(remaining) =>
        log.info("Sending response chunk ...")
        context.system.scheduler.scheduleOnce(100 millis span) {
          client ! MessageChunk(DateTime.now.toIsoDateTimeString + ", ").withAck(Ok(remaining - 1))
        }

      case x: Http.ConnectionClosed =>
        log.info("Canceling response stream due to {} ...", x)
        context.stop(self)
    }
  }

  // simple case class whose instances we use as send confirmation message for streaming chunks
  case class Ok(remaining: Int)

}