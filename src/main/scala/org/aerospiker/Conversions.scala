package org.aerospiker

import com.aerospike.client.{ Value, Record => AsRecord }
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
    case v: Boolean => new Value.BooleanValue(v)
    case v: Int => new Value.IntegerValue(v)
    case v: Long => new Value.LongValue(v)
    case v: Float => new Value.FloatValue(v)
    case v: Double => new Value.DoubleValue(v)
    case v: String => new Value.StringValue(v)
    case v: Array[Byte] => new Value.BytesValue(v)
    case v: Seq[_] => new Value.ListValue(v map { _toValue(_).getObject() })
    case v: Map[_, _] => new Value.MapValue(v mapValues { _toValue(_).getObject() })
    case v: Empty => new Value.NullValue()
    case _ => new Value.BlobValue(v)
  }

}
