package org.aerospiker

import com.aerospike.client.{ Host => AsHost, Key => AsKey, Bin => AsBin, Value => AsValue, Record => AsRecord }
import java.util.{ Map => JMap, List => JList }
import java.lang.{ Long => JLong, Double => JDouble, Boolean => JBool }

import scala.collection.mutable.{ Buffer => MBuffer, Map => MMap }
import scala.collection.JavaConversions._
import policy._

object Conversions {

  implicit class AsRecordConversion(x: AsRecord) {
    def toRecordOption: Option[Record] = {
      def convert(v: Any): Any = v match {
        case x: JBool => x: Boolean
        case x: JList[_] => { x: MBuffer[_] } map { convert(_) }
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

  implicit class KeyConversion(x: Key) {
    def toAsKey: AsKey = {
      new AsKey(x.namespace, x.set, x.key)
    }
  }

  implicit class ValueToAsValue(x: Value[_]) {
    def toAsValue: AsValue = {
      def _toAsValue(v: Any): AsValue = v match {
        case v: Int => new AsValue.IntegerValue(v)
        case v: String => new AsValue.StringValue(v)
        case v: Array[Byte] => new AsValue.BytesValue(v)
        case v: Long => new AsValue.LongValue(v)
        case v: List[_] => new AsValue.ListValue(v map { _toAsValue(_).getObject() })
        case v: Map[_, _] => new AsValue.MapValue(v mapValues { _toAsValue(_).getObject() })
        case null => new AsValue.NullValue()
        case v => new AsValue.BlobValue(v)
      }
      _toAsValue(x.value)
    }
  }

  implicit class BinConversion(x: Bin[_]) {
    def toAsBin: AsBin = {
      new AsBin(x.name, x.value.toAsValue)
    }
  }

}
