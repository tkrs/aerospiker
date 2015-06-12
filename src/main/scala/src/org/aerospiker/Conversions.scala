package org.aerospiker

import com.aerospike.client.{ Host => AsHost, Key => AsKey, Bin => AsBin, Value => AsValue, Record => AsRecord }

import scala.collection.mutable.{ Buffer => MBuffer, Map => MMap }
import scala.collection.JavaConversions._

import java.util.{ Map => JMap, List => JList }
import java.lang.{ Integer => JInteger, Long => JLong, Double => JDouble, Float => JFloat, Boolean => JBool }

object Conversions {

  implicit class XHost(x: Array[Host]) {
    def trans: Array[AsHost] = x map { h => new AsHost(h.name, h.port) }
  }

  implicit class XAsRecord(x: AsRecord) {
    def trans: Option[Record] = {
      def convert(v: Any): Any = v match {
        case x: JBool => x: Boolean
        case x: JList[_] => { x: MBuffer[_] } map { convert(_) }
        case x: JMap[String, _] => { x: MMap[String, _] } mapValues { convert(_) }
        case x: JFloat => x: Float
        case x: JDouble => x: Double
        case x: JInteger => x: Int
        case x: JLong => x: Long
        case x => x
      }

      x match {
        case null => None
        case a => a.bins match {
          case null => None
          case b => Some(Record(
            bins = b.mapValues(convert(_)) toMap,
            generation = x.generation,
            expiration = x.expiration))
        }
      }
    }
  }

  implicit class XKey(x: Key) {
    def trans: AsKey = {
      new AsKey(x.namespace, x.set, x.key)
    }
  }

  implicit class XBin(x: Bin[_]) {
    def trans: AsBin = {
      def toAsValue(v: Any): AsValue = v match {
        case v: Int => new AsValue.IntegerValue(v)
        case v: String => new AsValue.StringValue(v)
        case v: Array[Byte] => new AsValue.BytesValue(v)
        case v: Long => new AsValue.LongValue(v)
        case v: List[_] => new AsValue.ListValue(v map { toAsValue(_).getObject() })
        case v: Map[_, _] => new AsValue.MapValue(v mapValues { toAsValue(_).getObject() })
        case null => new AsValue.NullValue()
        case v => new AsValue.BlobValue(v)
      }
      val b = new AsBin(x.name, toAsValue(x.value.value))
      b
    }
  }

}
