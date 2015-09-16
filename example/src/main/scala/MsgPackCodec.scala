import aerospiker.msgpack.{ JsonPacker, JsonUnpacker }
import com.typesafe.scalalogging.LazyLogging
import io.circe._
import io.circe.jawn._

object MsgPackCodec extends App with LazyLogging {

  val json: String = s"""
                        |{
                        |  "id": "c730433b-082c-4984-9d66-855c243266f0",
                        |  "name": "Foo",
                        |  "n": null,
                        |  "positive": [${Int.MaxValue}, ${Short.MaxValue}, 0, ${Long.MaxValue}],
                        |  "negative": [${Int.MinValue}, ${Short.MinValue}, 0, ${Long.MinValue}],
                        |  "positive1": [${Float.MaxValue}, ${Double.MaxValue}],
                        |  "negative1": [${Float.MinValue}, ${Double.MinValue}],
                        |  "negative2": [-100.989889789966858],
                        |  "values": {
                        |    "bar": true,
                        |    "baz": 100.001,
                        |    "qux": ["a", "b"]
                        |  }
                        |}
                      """.stripMargin('|')

  val doc: Json = parse(json).getOrElse(Json.empty)

  val pack = JsonPacker.pack(doc)

  val ret = JsonUnpacker.unpackObjectMap(pack)

  logger.info(s"unpack ${ret.pretty(Printer.spaces2)}")

}
