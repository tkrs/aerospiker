package org.aerospiker
package json

import cats.data.{ Ior, NonEmptyList => NEL, Xor }
import com.aerospike.client.large.LargeMap
import com.typesafe.scalalogging.LazyLogging
import org.aerospiker.{ Client => AsClient }
import org.aerospiker.Conversions._
import org.aerospiker.policy._
import scala.collection.JavaConversions._
import scalaz.concurrent.Task

abstract class AerospikeLargeMap(val client: AsClient)(implicit cp: ClientPolicy) extends AerospikeLargeMapType with LazyLogging {

  protected def namespace: String
  protected def setName: String
  protected def key: String
  protected def binName: String

  private[this] lazy val lmap: LargeMap = {
    logger.debug(s"$namespace :: $setName :: $key")
    val k = Key(namespace, setName, key)
    client.asClient.getLargeMap(cp.writePolicyDefault, k, binName, null)
  }

  override def get[M](key: String)(implicit decoder: D[M]): Throwable Xor Option[M] = {
    Xor.fromTryCatch[Throwable] {
      lmap.get(key).toMap.asInstanceOf[Map[String, Any]]
    } map (m => decoder(m))
  }

  override def put[M](lmKey: String, m: M)(implicit encoder: E[M]): Throwable Xor Unit =
    Xor.fromTryCatch[Throwable] {
      logger.debug(m.toString)
      lmap.put(lmKey, encoder(m))
    }

  override def all[M](target: String, binNames: Seq[String])(implicit decoder: D[M]): Throwable Xor Seq[(Key, Option[M])] = ???

  override def deletes[M](keys: Seq[String]): Task[Seq[Xor[Throwable, Boolean]]] = ???

  override def puts[M](kvs: Map[String, M])(implicit encoder: E[M]): Task[Seq[Xor[Throwable, String]]] = ???

  override def delete[M](key: String): Xor[Throwable, Unit] = ???
}

