package org.aerospiker

import com.aerospike.client.{ Bin => AsBin, Key => AsKey, Record => AsRecord, AerospikeException }
import com.aerospike.client.listener.{ RecordListener, WriteListener }

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scalaz._

sealed trait Error

class ClientError(ex: AerospikeException) extends Error

class ResponseError(ex: AerospikeException) extends Error

trait Operation { self: BaseClient =>

  def put(key: Key, rec: Bin*): EitherT[Future, Error, Unit] =
    futuring[Error, Unit]({ ex =>
      new ResponseError(ex)
    })({ () =>
      val asKey = new AsKey(key.namespace, key.set, key.key)
      val asBin = rec map { a => new AsBin(a.name, a.value) }
      self.asClient.put(
        null,
        asKey,
        asBin: _*
      )
    })

  def get(key: Key): EitherT[Future, Error, AsRecord] =
    futuring[Error, AsRecord]({ ex =>
      new ResponseError(ex)
    })({ () =>
      val asKey = new AsKey(key.namespace, key.set, key.key)
      self.asClient.get(
        null,
        asKey
      )
    })

  private def futuring[B, A](e: AerospikeException => B)(f: () => A): EitherT[Future, B, A] =
    EitherT(Future {
      try {
        \/.right(f())
      } catch {
        case ex: AerospikeException => \/.left(e(ex))
      }
    })

}
