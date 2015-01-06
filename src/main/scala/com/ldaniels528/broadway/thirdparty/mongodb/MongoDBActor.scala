package com.ldaniels528.broadway.thirdparty.mongodb

import akka.actor.Actor
import com.ldaniels528.broadway.core.actors.Actors.BWxActorRef
import com.ldaniels528.broadway.core.actors.Actors.Implicits._
import com.ldaniels528.broadway.thirdparty.mongodb.MongoDBActor._
import com.mongodb.ServerAddress
import com.mongodb.casbah.Imports._

import scala.collection.concurrent.TrieMap

/**
 * The MongoDB Actor is capable of executing `find`, `findOne`, `insert`, `update`, and `upsert` commands
 * against a MongoDB server instance. NOTE: `find` and `findOne` queries require an actor a a recipient for retrieved
 * records.
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class MongoDBActor(databaseName: String, serverList: String) extends Actor {
  private val collections = TrieMap[String, MongoCollection]()
  private var conn_? : Option[MongoConnection] = None

  override def preStart() = conn_? = Option(getConnection(parseServerList(serverList)))

  override def postStop() {
    conn_?.foreach(_.close())
    conn_? = None
  }

  override def receive = {
    case Find(collection, query, fields, recipient) =>
      val mc = getCollection(collection)
      mc.foreach(_.find(query, fields) foreach { o =>
        recipient ! MongoResult(o)
      })

    case FindOne(collection, query, fields, recipient) =>
      val mc = getCollection(collection)
      mc.foreach(_.findOne(query, fields) foreach { o =>
        recipient ! MongoResult(o)
      })

    case Insert(collection, doc, concern) =>
      val mc = getCollection(collection)
      mc.foreach(_.insert(doc))

    case Update(collection, query, doc, multi, concern) =>
      val mc = getCollection(collection)
      mc.foreach(_.update(query, doc, upsert = false, multi, concern))

    case Upsert(collection, query, doc, multi, concern) =>
      val mc = getCollection(collection)
      mc.foreach(_.update(query, doc, upsert = true, multi, concern))

    case message =>
      unhandled(message)
  }

  private def getCollection(name: String) = conn_?.map(c => collections.getOrElseUpdate(name, c(databaseName)(name)))

}

/**
 * MongoDB Actor Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object MongoDBActor {

  /**
   * Creates a new database connection
   */
  def getConnection(addresses: List[ServerAddress]) = {
    // create the options
    val options = new MongoOptions()
    options.connectionsPerHost = 100
    options.maxWaitTime = 2000
    options.socketKeepAlive = false
    options.threadsAllowedToBlockForConnectionMultiplier = 50

    // create the connection
    MongoConnection(addresses, options)
  }

  /**
   * Parses a MongoDB server list
   * @param serverList the given MongoDB server list (e.g. "dev601:27017,dev602:27017,dev603:27017")
   * @return a [[List]] of [[ServerAddress]] objects
   */
  def parseServerList(serverList: String): List[ServerAddress] = {
    serverList.split("[,]").toList map { server =>
      server.split("[:]").toList match {
        case host :: port :: Nil => new ServerAddress(host, port.toInt)
        case s =>
          throw new IllegalArgumentException(s"Illegal server address '$s', expected format 'host:port'")
      }
    }
  }

  case class FindOne(collection: String, query: DBObject, fields: DBObject, recipient: BWxActorRef)

  case class Find(collection: String, query: DBObject, fields: DBObject, recipient: BWxActorRef)

  case class Insert(collection: String, doc: DBObject, concern: WriteConcern = WriteConcern.JournalSafe)

  case class MongoResult(o: DBObject)

  case class Update(collection: String,
                    query: DBObject,
                    doc: DBObject,
                    multi: Boolean = false,
                    concern: WriteConcern = WriteConcern.JournalSafe)

  case class Upsert(collection: String,
                    query: DBObject,
                    doc: DBObject,
                    multi: Boolean = false,
                    concern: WriteConcern = WriteConcern.JournalSafe)

}
