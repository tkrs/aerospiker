package aerospiker
package protocol

import java.io.UnsupportedEncodingException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

import com.aerospike.client.AerospikeException
import com.aerospike.client.command.{ ParticleType => PT }
import io.circe.Json

import scala.collection.mutable

object Buffer {

  def bytesToParticle(typ: Int, buf: Array[Byte]): Json = typ match {
    case PT.NULL => Json.Null
    case PT.STRING => Json.fromString(new String(buf, "UTF8").trim)
    case PT.INTEGER if buf.length == 8 => Json.fromLong(bytesToLong(buf))
    case PT.INTEGER if buf.length == 0 => Json.fromLong(0L)
    case PT.INTEGER if buf.length == 4 => Json.fromInt(bytesToLong(buf).toInt)
    case PT.INTEGER if buf.length > 8 => Json.fromBigDecimal(bytesToBigDecimal(buf))
    case PT.DOUBLE => Json.fromDoubleOrNull(bytesToDouble(buf))
    // case PT.BLOB => ???
    case PT.LIST => JsonUnpacker(ByteBuffer.wrap(buf)).unpack
    case PT.MAP => JsonUnpacker(ByteBuffer.wrap(buf)).unpack
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

  def longToBytes(v: Long): Array[Byte] =
    Array(v >>> 56 toByte, v >>> 48 toByte, v >>> 40 toByte, v >>> 32 toByte, v >>> 24 toByte, v >>> 16 toByte, v >>> 8 toByte, v >>> 0 toByte)

  def intToBytes(v: Int): Array[Byte] =
    Array(v >>> 24 toByte, v >>> 16 toByte, v >>> 8 toByte, v >>> 0 toByte)

  def shortToBytes(v: Short): Array[Byte] =
    Array(v >>> 8 toByte, v >>> 0 toByte)

  def stringToUtf8(s: String): Array[Byte] = if (s == null) Array.empty else {
    val length = s.length
    val builder: mutable.ArrayBuilder[Byte] = mutable.ArrayBuilder.make()
    for (i <- 0 until length) {
      val c = s.charAt(i)
      if (c < 0x80) {
        builder += c.toByte
      } else if (c < 0x800) {
        builder += (0xc0 | (c >> 6)).toByte
        builder += (0x80 | (c & 0x3f)).toByte
      } else {
        // Encountered a different encoding other than 2-byte UTF8. Let java handle it.
        try {
          return s.getBytes(UTF_8)
        } catch {
          case uee: UnsupportedEncodingException =>
            throw new RuntimeException("UTF8 encoding is not supported.", uee)
        }
      }
    }
    builder.result()
  }
}
