package org.aerospiker

import com.aerospike.client.{ Record => ARecord }
import java.util.{ Map => JMap, List => JList }
import java.lang.{ Long => JLong, Double => JDouble, Boolean => JBool }

import scala.collection.mutable.{ Buffer => MBuffer, Map => MMap }
import scala.collection.JavaConversions._

package object syntax {

  implicit class ARecordConversion(x: ARecord) {
    def asRecordOption: Option[Record] = {
      def convert(v: Any): Any = v match {
        case x: JBool => x: Boolean
        case x: JList[_] => ({ x: MBuffer[_] } map { convert(_) }).toList
        case x: JMap[String, _] => { x: MMap[String, _] } mapValues { convert(_) } toMap
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
          expiration = rec.expiration
        ))
      } yield r

    }
  }
}
