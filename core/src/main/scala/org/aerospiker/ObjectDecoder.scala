package org.aerospiker

trait ObjectDecoder[A] {
  def apply(o: Any): A
}

object ObjectDecoder {
  def apply[A](f: Any => A) = new ObjectDecoder[A] {
    def apply(o: Any): A = f(o)
  }

  implicit val idObjectDecoder = ObjectDecoder[Any](identity)

  implicit val stringObjectDecoder = ObjectDecoder[String] { String valueOf _ }

}
