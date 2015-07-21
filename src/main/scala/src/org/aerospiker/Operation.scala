package org.aerospiker

import com.aerospike.client.AerospikeException
import com.aerospike.client.policy.{
  Policy => AsPolicy,
  WritePolicy => AsWritePolicy
}

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scalaz._, Scalaz._

import Conversions._

sealed trait Error

class ClientError(ex: AerospikeException) extends Error

class ResponseError(ex: AerospikeException) extends Error

trait Operation { self: BaseClient =>

  def put(key: Key, rec: Bin[_]*)(
    implicit wp: AsWritePolicy = self.policy.writePolicyDefault): EitherT[Future, Error, Unit] =

    futurize[Error, Unit]({ ex =>
      new ResponseError(ex)
    })({ () =>
      val asKey = key.toAsKey
      val asBin = rec map (_.toAsBin) collect {
        case x => x
      }
      Some(self.asClient.put(
        wp,
        asKey,
        asBin: _*
      ))
    })

  def get(key: Key)(
    implicit rp: AsPolicy = self.policy.readPolicyDefault): EitherT[Future, Error, Record] =

    futurize[Error, Record]({ ex =>
      new ResponseError(ex)
    })({ () =>
      val asKey = key.toAsKey
      self.asClient.get(
        rp,
        asKey
      ).toRecordOption
    })

  private def futurize[B, A](e: AerospikeException => B)(f: () => Option[A]): EitherT[Future, B, A] =
    EitherT(Future {
      try {
        f() match {
          case Some(x) => x.right
          case None => e(new AerospikeException("null record responded")).left
        }
      } catch {
        case ex: AerospikeException => e(ex).left
      }
    })

}
