package org.aerospiker.codec

import cats.data.Xor
import org.aerospiker.Key

import scalaz.concurrent.Task

sealed trait Database {
  type D[A] <: Decoder[A]
  type E[A] <: Encoder[A]
  def get[M](key: String)(implicit decoder: D[M]): Throwable Xor Option[M]
  def all[M](target: String, binNames: Seq[String])(implicit decoder: D[M]): Throwable Xor Seq[(Key, Option[M])]
  def put[M](key: String, m: M)(implicit encoder: E[M]): Throwable Xor Unit
  def puts[M](kvs: Map[String, M])(implicit encoder: E[M]): Task[Seq[Throwable Xor String]]
  def delete[M](key: String): Throwable Xor Unit
  def deletes[M](keys: Seq[String]): Task[Seq[Throwable Xor Boolean]]
}

abstract class AerospikeType extends Database {
  override type D[A] = BinsDecoder[A]
  override type E[A] = BinsEncoder[A]
}

abstract class AerospikeLargeMapType extends Database {
  type D[A] = LDValueDecoder[A]
  type E[A] = LDValueEncoder[A]
}
