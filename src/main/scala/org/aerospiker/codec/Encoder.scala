package org.aerospiker.codec

import org.aerospiker.Bin

sealed trait Encoder[A]

trait BinsEncoder[A] extends Encoder[A] {
  def apply(m: A): Seq[Bin]
}
object BinsEncoder {
  def apply[A](f: A => Seq[Bin]) = new BinsEncoder[A] {
    def apply(m: A): Seq[Bin] = f(m)
  }
}

trait LDValueEncoder[A] extends Encoder[A] {
  def apply(m: A): LDValue
}
object LDValueEncoder {
  def apply[A](f: A => LDValue) = new LDValueEncoder[A] {
    def apply(m: A): LDValue = f(m)
  }
}
