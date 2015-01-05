package com.ldaniels528.broadway.thirdparty.mongodb

import akka.actor.Actor
import com.mongodb.ServerAddress
import com.mongodb.casbah.Imports._

import scala.collection.concurrent.TrieMap

/**
 * MongoDB Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class MongoDBActor(databaseName: String, addresses: ServerAddress*) extends Actor {
  private val collections = TrieMap[String, MongoCollection]()
  private var conn_? : Option[MongoConnection] = None

  override def preStart() = conn_? = Option(getConnection(addresses))

  override def postStop() {
    conn_?.foreach(_.close())
    conn_? = None
  }

  override def receive = {
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
  def getConnection(addresses: Seq[ServerAddress]) = {
    // create the options
    val options = new MongoOptions()
    options.connectionsPerHost = 100
    options.maxWaitTime = 2000
    options.socketKeepAlive = false
    options.threadsAllowedToBlockForConnectionMultiplier = 50

    // create the connection
    MongoConnection(addresses.toList, options)
  }

  case class Insert(collection: String, doc: DBObject, concern: WriteConcern = WriteConcern.JournalSafe)

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
