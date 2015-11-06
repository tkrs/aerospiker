package aerospiker.msgpack

import java.util.UUID

import cats.data.Xor
import org.scalatest._
import io.circe.Json
import io.circe.parse._

class MessagePackSpec extends FunSpec with BeforeAndAfter with BeforeAndAfterEach with Matchers {

  val json: String = s"""
                        |{
                        |  "id": "${UUID.randomUUID()}",
                        |  "option": null,
                        |  "intp": [${Int.MaxValue}, 0],
                        |  "intn": [${Int.MinValue}, 0],
                        |  "floatp": ${Float.MaxValue},
                        |  "floatn": ${Float.MinValue},
                        |  "longp": ${Long.MaxValue},
                        |  "longn": ${Long.MinValue},
                        |  "shotp": ${Short.MaxValue},
                        |  "shotn": ${Short.MinValue},
                        |  "doublep": ${Double.MaxValue},
                        |  "doublen": ${Double.MinValue},
                        |  "values": {
                        |    "bar": true,
                        |    "baz": 100.001,
                        |    "qux": ["a", "b"]
                        |  }
                        |}
                      """.stripMargin('|').trim

  case class Values(bar: Boolean, baz: Double, qux: Seq[String])
  case class Doc(
    id: String,
    option: Option[String],
    intp: Seq[Int],
    intn: Seq[Int],
    floatp: Float,
    floatn: Float,
    longp: Long,
    longn: Long,
    shortp: Short,
    shortn: Short,
    doublen: Double,
    doublep: Double,
    values: Values
  )

  describe("pack/unpack") {

    it("should be sames the  unpacked Json and the parsed Json") {
      val doc = parse(json) match {
        case Xor.Left(e) =>
          println(e); Json.empty
        case Xor.Right(v) => v
      }
      val packed = JsonPacker.pack(doc)
      val unpacked = JsonUnpacker.unpackObjectMap(packed)
      unpacked should be equals doc

    }

  }

}
