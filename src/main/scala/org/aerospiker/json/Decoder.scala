package org.aerospiker.json

sealed trait Decoder[A]

trait BinsDecoder[A] extends Decoder[A] {
  def apply(bin: Map[BinName, Any]): Option[A]
}
object BinsDecoder {
  def apply[A](f: Map[BinName, Any] => Option[A]) = new BinsDecoder[A] {
    def apply(m: Map[BinName, Any]): Option[A] = f(m)
  }
}

trait LDValueDecoder[A] extends Decoder[A] {
  def apply(bin: LDValue): Option[A]
}
object LDValueDecoder {
  def apply[A](f: LDValue => Option[A]) = new LDValueDecoder[A] {
    def apply(m: LDValue): Option[A] = f(m)
  }
}
