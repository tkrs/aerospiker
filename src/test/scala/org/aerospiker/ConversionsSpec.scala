package org.aerospiker

import com.aerospike.client.{ Value => AsValue }

import org.scalatest._
import policy._
import scalaz._
import scalaz.concurrent._

class ConversionsSpec extends FlatSpec with Matchers {

  import Conversions.anyToValue

  "Conversions#anyToValue" should "conver" in {

    val aa = anyToValue("tkrs").getObject() //
    aa shouldBe a[java.lang.String]

    val bb = anyToValue(true).getObject()
    bb shouldBe a[java.lang.Boolean]

    val cc = anyToValue(123456789).getObject()
    cc shouldBe a[java.lang.Integer]

    val dd = anyToValue(18984378939077L).getObject()
    dd shouldBe a[java.lang.Long]

    val ee = anyToValue(0.9876f).getObject()
    ee shouldBe a[java.lang.Float]

    val ff = anyToValue(0.98768978743796999243).getObject()
    ff shouldBe a[java.lang.Double]

    val gg = anyToValue("白").getObject()
    gg shouldBe a[java.lang.String]

    val hh = anyToValue(Seq("rust", "scala", "haskell")).getObject()
    hh shouldBe a[java.util.List[_]]

    val hhh = hh.asInstanceOf[java.util.List[_]]
    val hhhh: scala.collection.mutable.Buffer[_] = {
      import scala.collection.JavaConversions._
      hhh
    }
    hhhh foreach {
      _ shouldBe a[java.lang.String]
    }

    val ii = anyToValue(Array(0x00.toByte, 0x01.toByte)).getObject() // shouldBe a[Array[java.lang.Integer]]
    ii shouldBe a[Array[Byte]]

    val jj = anyToValue(List(Map(1 -> 2))).getObject()
    val jjj = jj.asInstanceOf[java.util.List[_]]
    val jjjj: scala.collection.mutable.Buffer[_] = {
      import scala.collection.JavaConversions._
      jjj
    }
    jjjj foreach {
      _ shouldBe a[java.util.Map[_, _]]
    }

    val kk = anyToValue(Empty()).getObject()
    assert(kk == null)

    case class L(o: String)
    val l = L("llllll")

    val ll = anyToValue(l).getObject()
    ll shouldBe a[L]

  }

}
