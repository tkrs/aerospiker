package org.aerospiker

import com.aerospike.client.{ AerospikeException, Language }
import com.aerospike.client.task.RegisterTask

import scala.reflect.ClassTag
import scalaz._, Scalaz._
import scalaz.concurrent._

import Conversions._
import policy._

sealed trait Error

class ClientError(ex: Throwable) extends Error

class ResponseError(ex: Throwable) extends Error

trait Operation { self: BaseClient =>

  def put(key: Key, rec: Bin*)(
    implicit wp: WritePolicy = self.policy.writePolicyDefault): EitherT[Future, Error, Unit] =
    futurize[Error, Unit]({ ex =>
      new ResponseError(ex)
    })({ () =>
      self.asClient.put(wp, key, rec: _*).some
    })

  def append(key: Key, rec: Bin*)(
    implicit wp: WritePolicy = self.policy.writePolicyDefault): EitherT[Future, Error, Unit] =
    futurize[Error, Unit]({ ex =>
      new ResponseError(ex)
    })({ () =>
      self.asClient.append(wp, key, rec: _*).some
    })

  def prepend(key: Key, rec: Bin*)(
    implicit wp: WritePolicy = self.policy.writePolicyDefault): EitherT[Future, Error, Unit] =
    futurize[Error, Unit]({ ex =>
      new ResponseError(ex)
    })({ () =>
      self.asClient.prepend(wp, key, rec: _*).some
    })

  def add(key: Key, rec: Bin*)(
    implicit wp: WritePolicy = self.policy.writePolicyDefault): EitherT[Future, Error, Unit] =
    futurize[Error, Unit]({ ex =>
      new ResponseError(ex)
    })({ () =>
      self.asClient.add(wp, key, rec: _*).some
    })

  def touch(key: Key)(
    implicit wp: WritePolicy = self.policy.writePolicyDefault): EitherT[Future, Error, Unit] =
    futurize[Error, Unit]({ ex =>
      new ResponseError(ex)
    })({ () =>
      self.asClient.touch(wp, key).some
    })

  def delete(key: Key)(
    implicit wp: WritePolicy = self.policy.writePolicyDefault): EitherT[Future, Error, Boolean] =
    futurize[Error, Boolean]({ ex =>
      new ResponseError(ex)
    })({ () =>
      self.asClient.delete(wp, key).some
    })

  def exists(key: Key)(
    implicit wp: WritePolicy = self.policy.writePolicyDefault): EitherT[Future, Error, Boolean] =
    futurize[Error, Boolean]({ ex =>
      new ResponseError(ex)
    })({ () =>
      self.asClient.exists(wp, key).some
    })

  def get(key: Key, binNames: String*)(
    implicit rp: Policy = self.policy.readPolicyDefault): EitherT[Future, Error, Record] =
    futurize[Error, Record]({ ex =>
      new ResponseError(ex)
    })({ () =>
      if (binNames.isEmpty)
        self.asClient.get(rp, key).toRecordOption
      else
        self.asClient.get(rp, key, binNames: _*).toRecordOption
    })

  def getHeader(key: Key)(
    implicit rp: Policy = self.policy.readPolicyDefault): EitherT[Future, Error, Record] =
    futurize[Error, Record]({ ex =>
      new ResponseError(ex)
    })({ () =>
      self.asClient.getHeader(rp, key).toRecordOption
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

  def execute[R](key: Key, packageName: String, funcName: String, values: Value*)(
    implicit wp: WritePolicy = self.policy.writePolicyDefault,
    tag: ClassTag[R]): EitherT[Future, Error, R] =
    futurize[Error, R]({ ex =>
      new ResponseError(ex)
    })({ () =>
      self.asClientW.execute(tag.runtimeClass.asInstanceOf[Class[R]], wp, key, packageName, funcName, values: _*).some
    })

  private def futurize[B, A](e: Throwable => B)(f: () => Option[A]) =
    EitherT[Future, B, A](Future {
      try {
        f() match {
          case Some(x) => x.right
          case None => e(new AerospikeException("null record responded")).left
        }
      } catch {
        case ex: Throwable => e(ex).left
      }
    })

}
