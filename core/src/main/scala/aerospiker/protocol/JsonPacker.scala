package aerospiker
package protocol

import java.nio.CharBuffer
import java.nio.charset.CharsetEncoder
import java.nio.charset.StandardCharsets.UTF_8

import com.aerospike.client.AerospikeException
import com.aerospike.client.command.ParticleType
import io.circe._

import scala.collection.mutable

/**
 * Serialize collection objects using MessagePack format specification:
 *
 * https://github.com/msgpack/msgpack/blob/master/spec.md
 */
object JsonPacker {

  private[this] val encoder: ThreadLocal[CharsetEncoder] = new ThreadLocal[CharsetEncoder] {
    override def initialValue(): CharsetEncoder = UTF_8.newEncoder()
  }

  def apply(): JsonPacker = new JsonPacker

  def double(x: BigDecimal): Boolean = x.scale != 0

  def formatArrayFamilyHeader(size: Int, builder: mutable.ArrayBuilder[Byte]): Unit = {
    if (size < 16)
      builder += (0x90 | size).toByte
    else if (size < 65536) {
      builder += 0xdc.toByte
      builder += (size >>> 8).toByte
      builder += (size >>> 0).toByte
    } else {
      builder += 0xdd.toByte
      builder += (size >>> 24).toByte
      builder += (size >>> 16).toByte
      builder += (size >>> 8).toByte
      builder += (size >>> 0).toByte
    }
  }

  def formatMapFamilyHeader(sz: Int, builder: mutable.ArrayBuilder[Byte]): Unit = {
    if (sz < 16)
      builder += (0x80 | sz).toByte
    else if (sz < 65536) {
      builder += 0xde.toByte
      builder += (sz >>> 8).toByte
      builder += (sz >>> 0).toByte
    } else {
      builder += 0xdf.toByte
      builder += (sz >>> 24).toByte
      builder += (sz >>> 16).toByte
      builder += (sz >>> 8).toByte
      builder += (sz >>> 0).toByte
    }
  }

  def formatNil(builder: mutable.ArrayBuilder[Byte]): Unit = builder += 0xc0.toByte

  def formatBoolFamily(v: Boolean, builder: mutable.ArrayBuilder[Byte]): Unit =
    builder += (if (v) 0xc3 else 0xc2).toByte

  def formatIntFamily(l: Long, builder: mutable.ArrayBuilder[Byte]): Unit =
    if (4294967296L <= l) formatLong(0xcf.toByte, l, builder)
    else if (65536L <= l) formatInt(0xce.toByte, l.toInt, builder)
    else if (256L <= l) formatShort(0xcd.toByte, l.toInt, builder)
    else if (128 <= l) formatByte(0xcc.toByte, l.toByte, builder)
    else if (0 <= l) formatByte(l.toByte, builder)
    else if (l >= -32L) formatByte((0xe0 | (l + 32)).toByte, builder)
    else if (l >= Byte.MinValue.toLong) formatByte(0xd0.toByte, l.toInt.toByte, builder)
    else if (l >= Short.MinValue.toLong) formatShort(0xd1.toByte, l.toInt, builder)
    else if (l >= Int.MinValue.toLong) formatInt(0xd2.toByte, l.toInt, builder)
    else formatLong(0xd3.toByte, l, builder)

  def formatIntFamily(t: Byte, v: BigInt, builder: mutable.ArrayBuilder[Byte]): Unit = {
    builder += t
    builder += (v >> 56).toByte
    builder += (v >> 48).toByte
    builder += (v >> 40).toByte
    builder += (v >> 32).toByte
    builder += (v >> 24).toByte
    builder += (v >> 16).toByte
    builder += (v >> 8).toByte
    builder += (v >> 0).toByte
  }

  def formatFloatFamily(v: Double, builder: mutable.ArrayBuilder[Byte]): Unit = {
    val x = java.lang.Double.doubleToLongBits(v)
    builder += 0xcb.toByte
    builder += (x >>> 56).toByte
    builder += (x >>> 48).toByte
    builder += (x >>> 40).toByte
    builder += (x >>> 32).toByte
    builder += (x >>> 24).toByte
    builder += (x >>> 16).toByte
    builder += (x >>> 8).toByte
    builder += (x >>> 0).toByte
  }

  def formatStrFamilyHeader(sz: Int, builder: mutable.ArrayBuilder[Byte]): Unit =
    if (sz < 32) {
      builder += (0xa0 | sz).toByte
      builder += ParticleType.STRING.toByte
    } else if (sz < 65536) {
      builder += 0xda.toByte
      builder += (sz >>> 8).toByte
      builder += (sz >>> 0).toByte
      builder += ParticleType.STRING.toByte
    } else {
      builder += 0xdb.toByte
      builder += (sz >>> 24).toByte
      builder += (sz >>> 16).toByte
      builder += (sz >>> 8).toByte
      builder += (sz >>> 0).toByte
      builder += ParticleType.STRING.toByte
    }

  def formatStrFamily(v: String, builder: mutable.ArrayBuilder[Byte]): Unit = {
    val cb = CharBuffer.wrap(v)
    val buf = encoder.get.encode(cb)
    val len = buf.remaining() + 1
    formatStrFamilyHeader(len, builder)
    val arr = Array.ofDim[Byte](len - 1)
    buf.get(arr)
    builder ++= arr
    buf.clear()
    cb.clear()
  }

  def formatByte(v: Byte, builder: mutable.ArrayBuilder[Byte]): Unit = builder += v

  def formatByte(t: Byte, v: Byte, builder: mutable.ArrayBuilder[Byte]): Unit = {
    builder += t
    builder += v
  }
  def formatShort(t: Byte, v: Int, builder: mutable.ArrayBuilder[Byte]): Unit = {
    builder += t
    builder += (v >>> 8).toByte
    builder += (v >>> 0).toByte
  }
  def formatInt(t: Byte, v: Int, builder: mutable.ArrayBuilder[Byte]): Unit = {
    builder += t
    builder += (v >>> 24).toByte
    builder += (v >>> 16).toByte
    builder += (v >>> 8).toByte
    builder += (v >>> 0).toByte
  }
  def formatLong(t: Byte, v: Long, builder: mutable.ArrayBuilder[Byte]): Unit = {
    builder += t
    builder += (v >>> 56).toByte
    builder += (v >>> 48).toByte
    builder += (v >>> 40).toByte
    builder += (v >>> 32).toByte
    builder += (v >>> 24).toByte
    builder += (v >>> 16).toByte
    builder += (v >>> 8).toByte
    builder += (v >>> 0).toByte
  }

}

final class JsonPacker {
  import JsonPacker._

  def encode[A](a: A)(implicit A: Encoder[A]): Either[AerospikeException, Array[Byte]] = pack(A(a))

  def pack(doc: Json): Either[AerospikeException, Array[Byte]] = {
    try {
      val acc: mutable.ArrayBuilder[Byte] = mutable.ArrayBuilder.make[Byte]
      go(doc, acc)
      Right(acc.result())
    } catch {
      case e: AerospikeException => Left(e)
    }
  }

  private[this] def go(json: Json, acc: mutable.ArrayBuilder[Byte]): Unit = json.fold[Unit](
    formatNil(acc),
    formatBoolFamily(_, acc),
    _.toBigDecimal match {
      case None => ()
      case Some(v) if double(v) =>
        formatFloatFamily(v.toDouble, acc)
      case Some(v) if v.isValidLong =>
        formatIntFamily(v.toLong, acc)
      case Some(v) if v.signum == -1 =>
        formatIntFamily(0x3d.toByte, v.toBigInt(), acc)
      case Some(v) =>
        formatIntFamily(0xcf.toByte, v.toBigInt(), acc)
    },
    formatStrFamily(_, acc),
    xs => {
      formatArrayFamilyHeader(xs.size, acc)
      xs.foreach(go(_, acc))
    },
    x => {
      val xs = x.toList
      formatMapFamilyHeader(xs.size, acc)
      xs.foreach {
        case (key, (v)) =>
          formatStrFamily(key, acc)
          go(v, acc)
      }
    }
  )
}
