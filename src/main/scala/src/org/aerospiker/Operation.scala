package org.aerospiker

import com.aerospike.client.{ AerospikeException, Language, Key => AKey, Value => AValue }
import com.aerospike.client.task.{ ExecuteTask, RegisterTask }

import scala.language.reflectiveCalls
import scala.reflect.ClassTag
import scalaz._, Scalaz._
import scalaz.concurrent._

import Conversions._
import policy._

sealed trait Error

class ClientError(ex: Throwable) extends Error

class ResponseError(ex: Throwable) extends Error

trait Operation { self: BaseClient =>

  def put(key: Key, rec: Bin[_]*)(
    implicit wp: WritePolicy = self.policy.writePolicyDefault): EitherT[Future, Error, Unit] =
    futurize[Error, Unit]({ ex =>
      new ResponseError(ex)
    })({ () =>
      val asKey = key.toAsKey
      val asBin = rec map (_.toAsBin) collect {
        case x => x
      }
      self.asClient.put(wp, asKey, asBin: _*).some
    })

  def append(key: Key, rec: Bin[_]*)(
    implicit wp: WritePolicy = self.policy.writePolicyDefault): EitherT[Future, Error, Unit] =
    futurize[Error, Unit]({ ex =>
      new ResponseError(ex)
    })({ () =>
      val asKey = key.toAsKey
      val asBin = rec map (_.toAsBin) collect {
        case x => x
      }
      self.asClient.append(wp, asKey, asBin: _*).some
    })

  def prepend(key: Key, rec: Bin[_]*)(
    implicit wp: WritePolicy = self.policy.writePolicyDefault): EitherT[Future, Error, Unit] =
    futurize[Error, Unit]({ ex =>
      new ResponseError(ex)
    })({ () =>
      val asKey = key.toAsKey
      val asBin = rec map (_.toAsBin) collect {
        case x => x
      }
      self.asClient.prepend(wp, asKey, asBin: _*).some
    })

  def add(key: Key, rec: Bin[_]*)(
    implicit wp: WritePolicy = self.policy.writePolicyDefault): EitherT[Future, Error, Unit] =
    futurize[Error, Unit]({ ex =>
      new ResponseError(ex)
    })({ () =>
      val asKey = key.toAsKey
      val asBin = rec map (_.toAsBin) collect {
        case x => x
      }
      self.asClient.add(wp, asKey, asBin: _*).some
    })

  def touch(key: Key)(
    implicit wp: WritePolicy = self.policy.writePolicyDefault): EitherT[Future, Error, Unit] =
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
    implicit wp: WritePolicy = self.policy.writePolicyDefault): EitherT[Future, Error, Boolean] =
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
    implicit wp: WritePolicy = self.policy.writePolicyDefault): EitherT[Future, Error, Boolean] =
    futurize[Error, Boolean]({ ex =>
      new ResponseError(ex)
    })({ () =>
      val asKey = key.toAsKey
      self.asClient.exists(wp, asKey).some
    })

  def get(key: Key, binNames: String*)(
    implicit rp: Policy = self.policy.readPolicyDefault): EitherT[Future, Error, Record] =
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
    implicit rp: Policy = self.policy.readPolicyDefault): EitherT[Future, Error, Record] =
    futurize[Error, Record]({ ex =>
      new ResponseError(ex)
    })({ () =>
      val asKey = key.toAsKey
      self.asClient.getHeader(rp, asKey).toRecordOption
    })

  def register(resourcePath: String, serverPath: String)(
    implicit rp: Policy = self.policy.readPolicyDefault,
    cl: ClassLoader = getClass.getClassLoader,
    lang: Language = Language.LUA): EitherT[Future, Error, RegisterTask] =
    futurize[Error, RegisterTask]({ ex =>
      new ResponseError(ex)
    })({ () =>
      self.asClient.register(rp, cl, resourcePath, serverPath, lang).some
    })

  def removeUdf(serverPath: String)(
    implicit ip: InfoPolicy = self.policy.infoPolicyDefault): EitherT[Future, Error, Unit] =
    futurize[Error, Unit]({ ex =>
      new ResponseError(ex)
    })({ () =>
      self.asClient.removeUdf(ip, serverPath).some
    })

  def execute[R](key: Key, packageName: String, funcName: String, values: Value[_]*)(
    implicit wp: WritePolicy = self.policy.writePolicyDefault,
    tag: ClassTag[R]): EitherT[Future, Error, R] =
    futurize[Error, R]({ ex =>
      new ResponseError(ex)
    })({ () =>
      val xs = values map (x => x.toAsValue)
      self.asClientW.execute(tag.runtimeClass.asInstanceOf[Class[R]], wp, key.toAsKey, packageName, funcName, xs: _*).some
    })

  private def futurize[B, A](e: Throwable => B)(f: () => Option[A]): EitherT[Future, B, A] =
    EitherT(Future {
      try {
        f() match {
          case Some(x) => x.right
          case None => e(new AerospikeException("null record responded")).left
        }
      } catch {
        case ex => e(ex).left
      }
    })

}
