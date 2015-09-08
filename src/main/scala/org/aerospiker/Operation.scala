package org.aerospiker

import com.aerospike.client.{ Key => AKey, Record => ARecord, AerospikeException, Language }
import com.aerospike.client.listener._
import com.aerospike.client.task.RegisterTask

import scalaz._, Scalaz._
import scalaz.concurrent.Task

sealed trait Error

case class ResponseError(ex: Throwable) extends Error

trait Operation { self: Client =>

  import Conversions._
  import policy.ClientPolicy

  def put[A](key: Key, bins: Bin*)(implicit cp: ClientPolicy) = EitherT[Task, Throwable, Unit] {
    Task.async[Unit] { register =>
      self.asClient.put(
        cp.asyncWritePolicyDefault,
        new WriteListener {
          def onSuccess(key: AKey): Unit = { register(\/-({})) }
          def onFailure(e: AerospikeException): Unit = { register(-\/(e)) }
        },
        key,
        bins: _*
      )
    } attempt
  }

  def append[A](key: Key, bins: Bin*)(implicit cp: ClientPolicy) = EitherT[Task, Throwable, Unit] {
    Task.async[Unit] { register =>
      self.asClient.append(
        cp.asyncWritePolicyDefault,
        new WriteListener {
          def onSuccess(key: AKey): Unit = { register(\/-({})) }
          def onFailure(e: AerospikeException): Unit = { register(-\/(e)) }
        },
        key,
        bins: _*
      )
    } attempt
  }

  def prepend[A](key: Key, bins: Bin*)(implicit cp: ClientPolicy) = EitherT[Task, Throwable, Unit] {
    Task.async[Unit] { register =>
      self.asClient.prepend(
        cp.asyncWritePolicyDefault,
        new WriteListener {
          def onSuccess(key: AKey): Unit = { register(\/-({})) }
          def onFailure(e: AerospikeException): Unit = { register(-\/(e)) }
        },
        key,
        bins: _*
      )
    } attempt
  }

  def add[A](key: Key, bins: Bin*)(implicit cp: ClientPolicy) = EitherT[Task, Throwable, Unit] {
    Task.async[Unit] { register =>
      self.asClient.add(
        cp.asyncWritePolicyDefault,
        new WriteListener {
          def onSuccess(key: AKey): Unit = { register(\/-({})) }
          def onFailure(e: AerospikeException): Unit = { register(-\/(e)) }
        },
        key,
        bins: _*
      )
    } attempt
  }

  def touch(key: Key)(implicit cp: ClientPolicy) = EitherT[Task, Throwable, Unit] {
    Task.async[Unit] { register =>
      self.asClient.touch(
        cp.asyncWritePolicyDefault,
        new WriteListener {
          def onSuccess(key: AKey): Unit = { register(\/-({})) }
          def onFailure(e: AerospikeException): Unit = { register(-\/(e)) }
        },
        key
      )
    } attempt
  }

  def delete(key: Key)(implicit cp: ClientPolicy) = EitherT[Task, Throwable, Boolean] {
    Task.async[Boolean] { register =>
      self.asClient.delete(
        cp.asyncWritePolicyDefault,
        new DeleteListener {
          def onSuccess(key: AKey, existed: Boolean): Unit = { register(\/-(existed)) }
          def onFailure(e: AerospikeException): Unit = { register(-\/(e)) }
        },
        key
      )
    } attempt
  }

  def exists(key: Key)(implicit cp: ClientPolicy) = EitherT[Task, Throwable, Boolean] {
    Task.async[Boolean] { register =>
      self.asClient.exists(
        cp.asyncWritePolicyDefault,
        new ExistsListener {
          def onSuccess(key: AKey, exists: Boolean): Unit = { register(\/-(exists)) }
          def onFailure(e: AerospikeException): Unit = { register(-\/(e)) }
        },
        key
      )
    } attempt
  }

  def get[A](key: Key, binNames: String*)(implicit cp: ClientPolicy) = EitherT[Task, Throwable, Option[Record]] {
    Task.async[Option[Record]] { register =>
      self.asClient.get(
        cp.asyncBatchPolicyDefault,
        new RecordArrayListener {
          def onSuccess(key: Array[AKey], records: Array[ARecord]): Unit = {
            register(\/-(records.toSeq.map(_.toRecordOption) match {
              case singleton @ Seq(x) => x
              case Seq(head, _*) => head
              case _ => None
            }))
          }
          def onFailure(e: AerospikeException): Unit = { register(-\/(e)) }
        },
        Array(key),
        binNames: _*
      )
    } attempt
  }

  def getHeader[A](keys: Array[Key])(implicit cp: ClientPolicy) = EitherT[Task, Throwable, Seq[(Key, Option[Record])]] {
    Task.async[Seq[(Key, Option[Record])]] { register =>
      self.asClient.getHeader(
        cp.batchPolicyDefault,
        new RecordArrayListener {
          def onSuccess(keys: Array[Key], records: Array[ARecord]): Unit = register(\/-(keys.zip(records.map(_.toRecordOption)).toSeq))
          def onFailure(e: AerospikeException): Unit = { register(-\/(e)) }
        },
        keys
      )
    } attempt
  }

  def register(resourcePath: String, serverPath: String)(
    implicit
    cp: ClientPolicy, cl: ClassLoader = getClass.getClassLoader, lang: Language = Language.LUA
  ) = EitherT[Task, Throwable, RegisterTask] {
    Task[RegisterTask] {
      self.asClient.register(cp.asyncWritePolicyDefault, cl, resourcePath, serverPath, lang)
    } attempt
  }

  def removeUdf(serverPath: String)(implicit cp: ClientPolicy) = EitherT[Task, Throwable, Unit] {
    Task[Unit] {
      self.asClient.removeUdf(cp.infoPolicyDefault, serverPath)
    } attempt
  }

  def scanAll[A](namespace: String, setName: String, binNames: String*)(implicit cp: ClientPolicy): Task[Throwable \/ Seq[(Key, Option[Record])]] =
    Task.async[Seq[(Key, Option[Record])]] { register =>
      import scala.collection.mutable.ListBuffer
      val buffer: ListBuffer[(Key, Option[Record])] = ListBuffer.empty
      self.asClient.scanAll(
        cp.asyncScanPolicyDefault,
        new RecordSequenceListener {
          def onRecord(key: Key, record: ARecord): Unit = buffer += key -> record.toRecordOption
          def onFailure(e: AerospikeException): Unit = register(e.left)
          def onSuccess(): Unit = register(buffer.toSeq.right)
        },
        namespace,
        setName,
        binNames: _*
      )
    } attempt

  def execute[R](key: Key, packageName: String, funcName: String, values: Value*)(
    implicit
    cp: ClientPolicy, decoder: ObjectDecoder[R]
  ) = EitherT[Task, Throwable, R] {
    Task.async[R] { register =>
      self.asClient.execute(
        cp.asyncWritePolicyDefault,
        new ExecuteListener {
          def onSuccess(keys: AKey, obj: Any): Unit = register(\/-(decoder(obj)))
          def onFailure(e: AerospikeException): Unit = { register(-\/(e)) }
        },
        key,
        packageName,
        funcName,
        values: _*
      )
    } attempt
  }

}
