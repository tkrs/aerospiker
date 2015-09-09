package org.aerospiker

trait ValueEncoder[A] {
  def apply(a: A): Value
}

object ValueEncoder {

  import com.aerospike.client.{ Value => AValue }
  import java.util.{ Map => JMap }
  import java.lang.{ Integer => JInt, Long => JLong, Float => JFloat, Double => JDouble, Boolean => JBool }
  import AValue._

  import scala.collection.JavaConversions._

  def apply[A](f: A => Value): ValueEncoder[A] = new ValueEncoder[A] {
    def apply(a: A): Value = f(a)
  }

  private[ValueEncoder] def m(a: Map[String, Object]): JMap[String, Object] = a

  private[ValueEncoder] def convert(a: Any): Object = a match {
    case z: Map[String, _] => m(z mapValues (convert(_)))
    case z: Seq[Any] => z map (convert(_))
    case z: Boolean => z: JBool
    case z: Int => z: JInt
    case z: Long => z: JLong
    case z: Float => z: JFloat
    case z: Double => z: JDouble
    case z: String => z
    case z: Array[Byte] => z
  }

  implicit val stringValueEncoder = ValueEncoder[String](v => new StringValue(v))

  implicit val boolValueEncoder = ValueEncoder[Boolean](v => new BlobValue(v))

  implicit val intValueEncoder = ValueEncoder[Int](v => new IntegerValue(v))

  implicit val longValueEncoder = ValueEncoder[Long](v => new LongValue(v))

  implicit val floatValueEncoder = ValueEncoder[Float](v => new BlobValue(v))

  implicit val doubleValueEncoder = ValueEncoder[Double](v => new BlobValue(v))

  implicit val bytesValueEncoder = ValueEncoder[Array[Byte]](v => new BytesValue(v))

  implicit val emptyValueEncoder = ValueEncoder[Empty](_ => new NullValue)

  implicit val mapValueEncoder = ValueEncoder[Map[String, Any]](v => new MapValue(v mapValues (convert(_))))

  implicit val seqValueEncoder = ValueEncoder[Seq[Any]](v => new ListValue(v map (convert(_))))

}
