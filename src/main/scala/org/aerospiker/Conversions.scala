package org.aerospiker

import com.aerospike.client.{ Value => AValue, Record => AsRecord }
import java.util.{ Map => JMap, List => JList }
import java.lang.{ Long => JLong, Double => JDouble, Boolean => JBool }

import scala.collection.mutable.{ Buffer => MBuffer, Map => MMap }
import scala.collection.JavaConversions._

object Conversions {

  implicit class AsRecordConversion(x: AsRecord) {
    def toRecordOption: Option[Record] = {
      def convert(v: Any): Any = v match {
        case x: JBool => x: Boolean
        case x: JList[_] => ({ x: MBuffer[_] } map { convert(_) }).toList
        case x: JMap[String, _] => { x: MMap[String, _] } mapValues { convert(_) }
        case x: JDouble => x: Double
        case x: JLong => x: Long
        case x => x
      }

      for {
        rec <- Option(x)
        b <- Option(rec.bins)
        r <- Some(Record(
          bins = b.mapValues(convert(_)) toMap,
          generation = rec.generation,
          expiration = rec.expiration))
      } yield r

    }
  }

  implicit def anyToValue(v: Any): Value = _toValue(v)

  private[this] def _toValue(v: Any): Value = v match {
    case v: Boolean => new AValue.BooleanValue(v)
    case v: Int => new AValue.IntegerValue(v)
    case v: Long => new AValue.LongValue(v)
    case v: Float => new AValue.FloatValue(v)
    case v: Double => new AValue.DoubleValue(v)
    case v: String => new AValue.StringValue(v)
    case v: Array[Byte] => new AValue.BytesValue(v)
    case v: Seq[_] => new AValue.ListValue(v map { _toValue(_).getObject() })
    case v: Map[_, _] => new AValue.MapValue(v mapValues { _toValue(_).getObject() })
    case v: Empty => new AValue.NullValue()
    case _ => new AValue.BlobValue(v)
  }

}
