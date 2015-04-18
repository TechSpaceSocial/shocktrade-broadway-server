package com.ldaniels528.broadway.core.actors.nosql

import akka.actor.ActorRef
import com.ldaniels528.broadway.core.actors.BroadwayActor
import com.ldaniels528.broadway.core.actors.nosql.MongoDBActor._
import com.ldaniels528.broadway.core.util.Counter
import com.mongodb.casbah.Imports.{DBObject => Q, _}
import com.mongodb.casbah.commons.conversions.scala.RegisterJodaTimeConversionHelpers
import com.mongodb.{DBObject, ServerAddress}

import scala.collection.concurrent.TrieMap

/**
 * The MongoDB Actor is capable of executing `find`, `findOne`, `insert`, `update`, and `upsert` commands against a
 * MongoDB server instance. NOTE: `find` and `findOne` queries require an actor a a recipient for retrieved records.
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class MongoDBActor(client: () => MongoClient, databaseName: String, counter: Option[Counter]) extends BroadwayActor {
  private val collections = TrieMap[String, MongoCollection]()
  private var conn_? : Option[MongoClient] = None

  // register the time/date helpers
  RegisterJodaTimeConversionHelpers()

  override def preStart() = conn_? = Option(client())

  override def postStop() {
    conn_?.foreach(_.close())
    conn_? = None
  }

  override def receive = {
    case Find(recipient, name, query, fields, maxBatchSize) =>
      getCollection(name).foreach(_.find(query, fields).sliding(maxBatchSize, maxBatchSize).foreach { docs =>
        recipient ! MongoFindResults(name, docs)
        counter.foreach(_ += docs.size)
      })

    case FindAndModify(recipient, name, query, fields, sort, update, remove, returnNew, upsert) =>
      getCollection(name)
        .foreach(_.findAndModify(query, fields, sort, remove, update, returnNew, upsert)
        .foreach { doc =>
        recipient ! MongoFindAndModifyResult(name, doc)
        counter.foreach(_ += 1)
      })

    case FindAndRemove(recipient, name, query) =>
      getCollection(name).foreach { mc =>
        recipient ! MongoFindOneResult(name, mc.findAndRemove(query))
        counter.foreach(_ += 1)
      }

    case FindOne(recipient, name, query, fields) =>
      getCollection(name).foreach { mc =>
        recipient ! MongoFindOneResult(name, mc.findOne(query, fields))
        counter.foreach(_ += 1)
      }

    case FindOneByID(recipient, name, id, fields) =>
      getCollection(name).foreach { mc =>
        recipient ! MongoFindOneResult(name, mc.findOneByID(id, fields))
        counter.foreach(_ += 1)
      }

    case Insert(recipient, name, doc, concern, refObj) =>
      getCollection(name).foreach { mc =>
        val result = mc.insert(doc, concern)
        recipient.foreach(_ ! MongoWriteResult(name, doc, result, refObj))
        counter.foreach(_ += 1)
      }

    case Save(recipient, name, doc, concern, refObj) =>
      getCollection(name).foreach { mc =>
        val result = mc.save(doc, concern)
        recipient.foreach(_ ! MongoWriteResult(name, doc, result, refObj))
        counter.foreach(_ += 1)
      }

    case Update(recipient, name, query, doc, multi, concern, refObj) =>
      getCollection(name).foreach { mc =>
        val result = mc.update(query, doc, upsert = false, multi, concern)
        recipient.foreach(_ ! MongoWriteResult(name, doc, result, refObj))
        counter.foreach(_ += 1)
      }

    case Upsert(recipient, name, query, doc, multi, concern, refObj) =>
      getCollection(name).foreach { mc =>
        val result = mc.update(query, doc, upsert = true, multi, concern)
        recipient.foreach(_ ! MongoWriteResult(name, doc, result, refObj))
        counter.foreach(_ += 1)
      }

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
  def apply(addresses: List[ServerAddress], databaseName: String, counter: Option[Counter] = None) = {
    new MongoDBActor(() => MongoClient(addresses), databaseName, counter)
  }

  /**
   * Parses a MongoDB server list
   * @param serverList the given MongoDB server list (e.g. "dev801:27017,dev802:27017,dev803:27017")
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

  case class Find(recipient: ActorRef, name: String, query: DBObject, fields: DBObject = Q(), maxFetchSize: Int = 32)

  case class FindOne(recipient: ActorRef, name: String, query: DBObject, fields: DBObject = Q())

  case class FindOneByID(recipient: ActorRef, name: String, id: String, fields: DBObject = Q())

  case class FindAndModify(recipient: ActorRef,
                           name: String,
                           query: DBObject,
                           fields: DBObject = Q(),
                           sort: DBObject = Q(),
                           update: DBObject,
                           remove: Boolean = false,
                           returnNew: Boolean = true,
                           upsert: Boolean = false)

  case class FindAndRemove(recipient: ActorRef, name: String, query: DBObject)

  case class Insert(recipient: Option[ActorRef],
                    name: String,
                    doc: DBObject,
                    concern: WriteConcern = WriteConcern.JournalSafe,
                    refObj: Option[Any] = None)

  case class Save(recipient: Option[ActorRef],
                  name: String,
                  doc: DBObject,
                  concern: WriteConcern = WriteConcern.JournalSafe,
                  refObj: Option[Any] = None)

  case class Update(recipient: Option[ActorRef],
                    name: String,
                    query: DBObject,
                    doc: DBObject,
                    multi: Boolean = false,
                    concern: WriteConcern = WriteConcern.JournalSafe,
                    refObj: Option[Any] = None)

  case class Upsert(recipient: Option[ActorRef],
                    name: String,
                    query: DBObject,
                    doc: DBObject,
                    multi: Boolean = false,
                    concern: WriteConcern = WriteConcern.JournalSafe,
                    refObj: Option[Any] = None)

  case class MongoFindOneResult(name: String, doc: Option[DBObject])

  case class MongoFindResults(name: String, docs: Seq[DBObject])

  case class MongoFindAndModifyResult(name: String, doc: DBObject)

  case class MongoWriteResult(name: String, doc: DBObject, result: WriteResult, refObj: Option[Any])

}
