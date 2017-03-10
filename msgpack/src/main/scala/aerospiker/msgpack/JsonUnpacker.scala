package aerospiker.msgpack

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

import cats.syntax.either._
import com.aerospike.client.AerospikeException
import com.aerospike.client.command.ParticleType
import io.circe.{ Decoder, DecodingFailure, Error, Json }

import scala.annotation.tailrec

/**
 * De-serialize collection objects using MessagePack format specification:
 *
 * https://github.com/msgpack/msgpack/blob/master/spec.md
 */
object JsonUnpacker {

  def apply(b: ByteBuffer) = new JsonUnpacker(b)

  def bytesToShort(buf: ByteBuffer, i: Int): Int =
    bytesToUShort(buf, i)

  def bytesToUShort(buf: ByteBuffer, i: Int): Int =
    ((buf.get(i) & 0xFF) << 8) |
      ((buf.get(i + 1) & 0xFF) << 0)

  def bytesToInt(buf: ByteBuffer, i: Int): Int =
    ((buf.get(i) & 0xFF) << 24) |
      ((buf.get(i + 1) & 0xFF) << 16) |
      ((buf.get(i + 2) & 0xFF) << 8) |
      ((buf.get(i + 3) & 0xFF) << 0)

  def bytesToUInt(buf: ByteBuffer, i: Int): Long =
    ((buf.get(i) & 0xFF).toLong << 24) |
      ((buf.get(i + 1) & 0xFF).toLong << 16) |
      ((buf.get(i + 2) & 0xFF).toLong << 8) |
      ((buf.get(i + 3) & 0xFF).toLong << 0)

  def bytesToLong(buf: ByteBuffer, i: Int): Long =
    ((buf.get(i) & 0xFF).toLong << 56) |
      ((buf.get(i + 1) & 0xFF).toLong << 48) |
      ((buf.get(i + 2) & 0xFF).toLong << 40) |
      ((buf.get(i + 3) & 0xFF).toLong << 32) |
      ((buf.get(i + 4) & 0xFF).toLong << 24) |
      ((buf.get(i + 5) & 0xFF).toLong << 16) |
      ((buf.get(i + 6) & 0xFF).toLong << 8) |
      ((buf.get(i + 7) & 0xFF).toLong << 0)

  def bytesToULong(buf: ByteBuffer, i: Int): BigInt =
    (BigInt(buf.get(i) & 0xFF) << 56) |
      (BigInt(buf.get(i + 1) & 0xFF) << 48) |
      (BigInt(buf.get(i + 2) & 0xFF) << 40) |
      (BigInt(buf.get(i + 3) & 0xFF) << 32) |
      (BigInt(buf.get(i + 4) & 0xFF) << 24) |
      (BigInt(buf.get(i + 5) & 0xFF) << 16) |
      (BigInt(buf.get(i + 6) & 0xFF) << 8) |
      (BigInt(buf.get(i + 7) & 0xFF) << 0)

  def utf8ToString(buf: ByteBuffer, offset: Int, length: Int): String = {
    val arr = Array.ofDim[Byte](length)
    buf.position(offset)
    buf.get(arr)
    new String(arr, UTF_8)
  }
}

final class JsonUnpacker(src: ByteBuffer) {
  import JsonUnpacker._

  private[this] var offset = src.position

  private[this] def readAt(i: Int): Int = {
    val v = src.get(offset).toInt
    offset += i
    v
  }

  private[this] def decodeAt[A](i: Int, f: (ByteBuffer, Int) => A): A = {
    val v = f(src, offset)
    offset += i
    v
  }

  def decode[A](implicit decoder: Decoder[A]): Either[Error, A] =
    Either.catchOnly[AerospikeException](unpack)
      .leftMap(e => DecodingFailure(e.getMessage, List.empty))
      .flatMap(_.as[A])

  def unpack: Json = readAt(1) & 0xff match {
    case 0xc0 => Json.Null
    case 0xc3 => Json.fromBoolean(true)
    case 0xc2 => Json.fromBoolean(false)
    case 0xca => Json.fromDoubleOrNull(java.lang.Float.intBitsToFloat(decodeAt(4, bytesToInt)).toDouble)
    case 0xcb => Json.fromDoubleOrNull(java.lang.Double.longBitsToDouble(decodeAt(8, bytesToLong)))
    case 0xd0 => Json.fromInt(readAt(1))
    case 0xcc => Json.fromInt(readAt(1) & 0xff)
    case 0xd1 => Json.fromInt(decodeAt(2, bytesToShort))
    case 0xcd => Json.fromInt(decodeAt(2, bytesToUShort))
    case 0xd2 => Json.fromInt(decodeAt(4, bytesToInt))
    case 0xce => Json.fromLong(decodeAt(4, bytesToUInt))
    case 0xd3 => Json.fromLong(decodeAt(8, bytesToLong))
    case 0xcf => Json.fromBigInt(decodeAt(8, bytesToULong))
    case 0xd9 => unpackBlob(readAt(1), readAt(1) & 0xff)
    case 0xda => unpackBlob(readAt(1), decodeAt(2, bytesToShort))
    case 0xdb => unpackBlob(readAt(1), decodeAt(4, bytesToInt))
    case 0xdc => unpackList(decodeAt(2, bytesToShort))
    case 0xdd => unpackList(decodeAt(4, bytesToInt))
    case 0xde => unpackMap(decodeAt(2, bytesToShort))
    case 0xdf => unpackMap(decodeAt(4, bytesToInt))
    case t if (t & 0xe0) == 0xa0 => unpackBlob(readAt(1), t & 0x1f)
    case t if (t & 0xf0) == 0x80 => unpackMap(t & 0x0f)
    case t if (t & 0xf0) == 0x90 => unpackList(t & 0x0f)
    case t if t < 0x80 => Json.fromLong(t.toLong)
    case t if t >= 0xe0 => Json.fromLong((t - 0xe0 - 32).toLong)
    case t => throw new AerospikeException.Serialize(new Exception("Unsupported type: 0x%02x".format(t)))
  }

  private[this] def unpackList(limit: Int): Json = {
    @tailrec def loop(i: Int, acc: Array[Json]): Array[Json] =
      if (i == limit) acc else {
        acc(i) = unpack
        loop(i + 1, acc)
      }
    Json.fromValues(loop(0, Array.ofDim[Json](limit)))
  }

  private[this] def unpackMap(size: Int): Json = {
    val key: Json => String = _.asString match {
      case Some(s) => s
      case None => throw new AerospikeException.Serialize(new Exception(s"Failed to decode key by the offset: $offset"))
    }
    def loop(i: Int, acc: Seq[(String, Json)]): Seq[(String, Json)] =
      if (i == 0) acc else loop(i - 1, acc :+ (key(unpack) -> unpack))
    Json.fromFields(loop(size, Seq.empty))
  }

  private[this] def unpackBlob(typ: Int, size: Int): Json = typ match {
    case ParticleType.STRING => unpackString(size)
    case ParticleType.JBLOB => throw new AerospikeException("Not support JBLOB in Aerospiker")
    // val bastream: ByteArrayInputStream = new ByteArrayInputStream(buffer, offset, c)
    // val oistream: ObjectInputStream = new ObjectInputStream(bastream)
    // val v = oistream.readObject
    // offset += c
    case _ => throw new AerospikeException("Not support other type in Aerospiker")
    // val v = Arrays.copyOfRange(buffer, offset, offset + c)
    // offset += c
    // (v)
  }

  private[this] def unpackString(size: Int): Json =
    Json.fromString(decodeAt(size, utf8ToString(_, _, size)))
}
