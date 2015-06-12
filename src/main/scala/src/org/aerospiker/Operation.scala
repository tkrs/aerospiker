package org.aerospiker

import com.aerospike.client.AerospikeException
import com.aerospike.client.listener.{ RecordListener, WriteListener }

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scalaz._

import Conversions._

sealed trait Error

class ClientError(ex: AerospikeException) extends Error

class ResponseError(ex: AerospikeException) extends Error

trait Operation { self: BaseClient =>

  def put(key: Key, rec: Bin[_]*): EitherT[Future, Error, Unit] =
    futuring[Error, Unit]({ ex =>
      new ResponseError(ex)
    })({ () =>
      val asKey = key.trans
      val asBin = rec map (_.trans) collect {
        case x => x
      }
      Some(self.asClient.put(
        null,
        asKey,
        asBin: _*
      ))
    })

  def get(key: Key): EitherT[Future, Error, Record] =
    futuring[Error, Record]({ ex =>
      new ResponseError(ex)
    })({ () =>
      val asKey = key.trans
      self.asClient.get(
        null,
        asKey
      ).trans
    })

  private def futuring[B, A](e: AerospikeException => B)(f: () => Option[A]): EitherT[Future, B, A] =
    EitherT(Future {
      try {
        f() match {
          case Some(x) => x match {
            case xx: A => \/.right(xx)
            case _ => \/.left(e(new AerospikeException("not found")))
          }
          case None => \/.left(e(new AerospikeException("respond null record")))
        }
      } catch {
        case ex: AerospikeException => \/.left(e(ex))
      }
    })

}
