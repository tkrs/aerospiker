package org.aerospiker

import com.aerospike.client.{ AerospikeException, Language, Value => AsValue }
import com.aerospike.client.task.{ ExecuteTask, RegisterTask }
import com.aerospike.client.policy.{
  Policy => AsPolicy,
  WritePolicy => AsWritePolicy
}

import scalaz._, Scalaz._
import scalaz.concurrent._

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

  def append(key: Key, rec: Bin[_]*)(
    implicit wp: AsWritePolicy = self.policy.writePolicyDefault): EitherT[Future, Error, Unit] =

    futurize[Error, Unit]({ ex =>
      new ResponseError(ex)
    })({ () =>
      val asKey = key.toAsKey
      val asBin = rec map (_.toAsBin) collect {
        case x => x
      }
      Some(self.asClient.append(
        wp,
        asKey,
        asBin: _*
      ))
    })

  def prepend(key: Key, rec: Bin[_]*)(
    implicit wp: AsWritePolicy = self.policy.writePolicyDefault): EitherT[Future, Error, Unit] =

    futurize[Error, Unit]({ ex =>
      new ResponseError(ex)
    })({ () =>
      val asKey = key.toAsKey
      val asBin = rec map (_.toAsBin) collect {
        case x => x
      }
      Some(self.asClient.prepend(
        wp,
        asKey,
        asBin: _*
      ))
    })

  def add(key: Key, rec: Bin[_]*)(
    implicit wp: AsWritePolicy = self.policy.writePolicyDefault): EitherT[Future, Error, Unit] =
    futurize[Error, Unit]({ ex =>
      new ResponseError(ex)
    })({ () =>
      val asKey = key.toAsKey
      val asBin = rec map (_.toAsBin) collect {
        case x => x
      }
      Some(self.asClient.add(
        wp,
        asKey,
        asBin: _*
      ))
    })

  def touch(key: Key)(
    implicit wp: AsWritePolicy = self.policy.writePolicyDefault): EitherT[Future, Error, Unit] =
    futurize[Error, Unit]({ ex =>
      new ResponseError(ex)
    })({ () =>
      val asKey = key.toAsKey
      Some(self.asClient.touch(
        wp,
        asKey
      ))
    })

  def delete(key: Key)(
    implicit wp: AsWritePolicy = self.policy.writePolicyDefault): EitherT[Future, Error, Boolean] =
    futurize[Error, Boolean]({ ex =>
      new ResponseError(ex)
    })({ () =>
      val asKey = key.toAsKey
      Some(self.asClient.delete(
        wp,
        asKey
      ))
    })

  def exists(key: Key)(
    implicit wp: AsWritePolicy = self.policy.writePolicyDefault): EitherT[Future, Error, Boolean] =
    futurize[Error, Boolean]({ ex =>
      new ResponseError(ex)
    })({ () =>
      val asKey = key.toAsKey
      Some(self.asClient.exists(
        wp,
        asKey
      ))
    })

  def get(key: Key, binNames: String*)(
    implicit rp: AsPolicy = self.policy.readPolicyDefault): EitherT[Future, Error, Record] =
    futurize[Error, Record]({ ex =>
      new ResponseError(ex)
    })({ () =>
      val asKey = key.toAsKey
      if (binNames.isEmpty)
        self.asClient.get(rp, asKey).toRecordOption
      else
        self.asClient.get(rp, asKey, binNames: _*).toRecordOption
    })

  def getHeader(key: Key)(
    implicit rp: AsPolicy = self.policy.readPolicyDefault): EitherT[Future, Error, Record] =

    futurize[Error, Record]({ ex =>
      new ResponseError(ex)
    })({ () =>
      val asKey = key.toAsKey
      self.asClient.getHeader(
        rp,
        asKey
      ).toRecordOption
    })

  // def register(resourcePath: String, serverPath: String)(
  //   implicit rp: AsPolicy = self.policy.readPolicyDefault,
  //   rl: ClassLoader = getClass.getClassLoader,
  //   lang: Language = Language.LUA): EitherT[Future, Error, RegisterTask] =
  //
  //   futurize[Error, RegisterTask]({ ex =>
  //     new ResponseError(ex)
  //   })({ () =>
  //     self.asClient.register(rp, rl, resourcePath, serverPath, lang).some
  //   })
  //
  // def execute(packageName: String, funcName: String, values: Value[_]*)(
  //   implicit wp: AsPolicy = self.policy.writePolicyDefault): EitherT[Future, Error, Object] =
  //   futurize[Error, Object]({ ex =>
  //     new ResponseError(ex)
  //   })({ () =>
  //     val xs = values map {
  //       x => x.toAsValue
  //     }
  //     self.asClient.execute(wp, packageName, funcName, xs).some
  //   })

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
