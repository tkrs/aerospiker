package org.aerospiker

import com.aerospike.client.{ AerospikeException, Language }
import com.aerospike.client.task.RegisterTask

import scala.reflect.ClassTag
import scalaz._, Scalaz._
import scalaz.concurrent._

import Conversions._
import policy._

sealed trait Error

case class ClientError(ex: Throwable) extends Error

case class ResponseError(ex: Throwable) extends Error

trait Operation { self: Client =>

  def put(key: Key, rec: Bin*)(implicit cp: ClientPolicy): EitherT[Future, Error, Unit] =
    futurize[Error, Unit](new ResponseError(_)) { () =>
      self.asClient.put(cp.writePolicyDefault, key, rec: _*).some
    }

  def append(key: Key, rec: Bin*)(implicit cp: ClientPolicy): EitherT[Future, Error, Unit] =
    futurize[Error, Unit](new ResponseError(_)) { () =>
      self.asClient.append(cp.writePolicyDefault, key, rec: _*).some
    }

  def prepend(key: Key, rec: Bin*)(implicit cp: ClientPolicy): EitherT[Future, Error, Unit] =
    futurize[Error, Unit](new ResponseError(_)) { () =>
      self.asClient.prepend(cp.writePolicyDefault, key, rec: _*).some
    }

  def add(key: Key, rec: Bin*)(implicit cp: ClientPolicy): EitherT[Future, Error, Unit] =
    futurize[Error, Unit](new ResponseError(_))({ () =>
      self.asClient.add(cp.writePolicyDefault, key, rec: _*).some
    })

  def touch(key: Key)(implicit cp: ClientPolicy): EitherT[Future, Error, Unit] =
    futurize[Error, Unit](new ResponseError(_))({ () =>
      self.asClient.touch(cp.writePolicyDefault, key).some
    })

  def delete(key: Key)(implicit cp: ClientPolicy): EitherT[Future, Error, Boolean] =
    futurize[Error, Boolean](new ResponseError(_)) { () =>
      self.asClient.delete(cp.writePolicyDefault, key).some
    }

  def exists(key: Key)(implicit cp: ClientPolicy): EitherT[Future, Error, Boolean] =
    futurize[Error, Boolean](new ResponseError(_))({ () =>
      self.asClient.exists(cp.writePolicyDefault, key).some
    })

  def get(key: Key, binNames: String*)(implicit cp: ClientPolicy): EitherT[Future, Error, Record] =
    futurize[Error, Record](new ResponseError(_))({ () =>
      if (binNames.isEmpty)
        self.asClient.get(cp.readPolicyDefault, key).toRecordOption
      else
        self.asClient.get(cp.readPolicyDefault, key, binNames: _*).toRecordOption
    })

  def getHeader(key: Key)(implicit cp: ClientPolicy): EitherT[Future, Error, Record] =
    futurize[Error, Record](new ResponseError(_))({ () =>
      self.asClient.getHeader(cp.readPolicyDefault, key).toRecordOption
    })

  def register(resourcePath: String, serverPath: String)(
    implicit cp: ClientPolicy,
    cl: ClassLoader = getClass.getClassLoader,
    lang: Language = Language.LUA): EitherT[Future, Error, RegisterTask] =
    futurize[Error, RegisterTask](new ResponseError(_))({ () =>
      self.asClient.register(cp.writePolicyDefault, cl, resourcePath, serverPath, lang).some
    })

  def removeUdf(serverPath: String)(implicit cp: ClientPolicy): EitherT[Future, Error, Unit] =
    futurize[Error, Unit](new ResponseError(_)) { () =>
      self.asClient.removeUdf(cp.infoPolicyDefault, serverPath).some
    }

  def execute[R](key: Key, packageName: String, funcName: String, values: Value*)(
    implicit cp: ClientPolicy,
    tag: ClassTag[R]): EitherT[Future, Error, R] =
    futurize[Error, R](new ResponseError(_)) { () =>
      val klass = tag.runtimeClass.asInstanceOf[Class[R]]
      self.asClientW.execute(klass, cp.writePolicyDefault, key, packageName, funcName, values: _*).some
    }

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
