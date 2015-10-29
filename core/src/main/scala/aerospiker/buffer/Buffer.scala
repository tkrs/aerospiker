package aerospiker
package buffer

import java.io.UnsupportedEncodingException
import java.nio.charset.StandardCharsets.UTF_8

import aerospiker.msgpack.JsonUnpacker
import com.aerospike.client.AerospikeException
import com.aerospike.client.command.{ ParticleType => PT }
import io.circe.Json
import scala.collection.mutable.ListBuffer

object Buffer {

  def bytesToParticle(typ: Int, buf: Array[Byte]): Json = typ match {
    case PT.NULL => Json.empty
    case PT.STRING => Json.string(new String(buf, "UTF8").trim)
    case PT.INTEGER if buf.length == 8 => Json.long(bytesToLong(buf))
    case PT.INTEGER if buf.length == 0 => Json.long(0L)
    case PT.INTEGER if buf.length == 4 => Json.int(bytesToLong(buf).toInt)
    case PT.INTEGER if buf.length > 8 => Json.bigDecimal(bytesToBigDecimal(buf))
    case PT.DOUBLE => Json.numberOrNull(bytesToDouble(buf))
    // case PT.BLOB => ???
    case PT.LIST => JsonUnpacker.unpackObjectList(buf)
    case PT.MAP => JsonUnpacker.unpackObjectMap(buf)
    case _ => throw new AerospikeException(s"Unsupported particle type[$typ]") // TODO: error message
  }

  def bytesToBigDecimal(buf: Array[Byte]): BigDecimal = {
    var negative = false
    if ((buf(0) & 0x80) != 0) {
      negative = true
      buf.update(0, (buf(0) % 0x7f).toByte) // TODO: ???
    }
    val bytes = new Array[Byte](buf.length)
    System.arraycopy(buf, 0, bytes, 0, buf.length)
    val n = new String(bytes, "UTF8").trim
    val big = if (n.isEmpty) BigDecimal(0) else BigDecimal(n)
    if (negative) BigDecimal(0) - big else big
  }

  def bytesToLong(buf: Array[Byte]): Long =
    buf.foldRight((0, 0L))((l, r) => r match {
      case (i, n) => (i + 8, ((l & 0xff).toLong << i) | n)
    })._2

  def bytesToInt(buf: Array[Byte]): Int = bytesToLong(buf).toInt

  def bytesToShort(buf: Array[Byte]): Short = bytesToLong(buf).toShort

  def bytesToDouble(buf: Array[Byte]): Double =
    java.lang.Double.longBitsToDouble(bytesToLong(buf))

  def longToBytes(v: Long): Seq[Byte] =
    Seq(v >>> 56, v >>> 48, v >>> 40, v >>> 32, v >>> 24, v >>> 16, v >>> 8, v >>> 0).map(_.toByte)

  def intToBytes(v: Int): Seq[Byte] =
    Seq(v >>> 24, v >>> 16, v >>> 8, v >>> 0).map(_.toByte)

  def shortToBytes(v: Short): Seq[Byte] =
    Seq(v >>> 8, v >>> 0).map(_.toByte)

  def stringToUtf8(s: String): (Array[Byte], Int) = {
    if (s == null) (Array.empty, 0)
    else {
      val length = s.length
      val buf: ListBuffer[Byte] = ListBuffer.empty
      for (i <- 0 until length) {
        val c = s.charAt(i)
        if (c < 0x80) {
          buf += c.toByte
        } else if (c < 0x800) {
          buf += (0xc0 | (c >> 6)).toByte
          buf += (0x80 | (c & 0x3f)).toByte
        } else {
          // Encountered a different encoding other than 2-byte UTF8. Let java handle it.
          try {
            val value = s.getBytes(UTF_8)
            return (value, value.length)
          } catch {
            case uee: UnsupportedEncodingException =>
              throw new RuntimeException("UTF8 encoding is not supported.")
          }
        }
      }
      (buf.toArray, buf.length)
    }
  }
}
