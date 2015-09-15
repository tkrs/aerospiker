package aerospiker
package msgpack

import com.aerospike.client.AerospikeException
import com.aerospike.client.AerospikeException.Serialize
import com.aerospike.client.command.{ Buffer, ParticleType }

import io.circe._

// TODO: Improve implement

/**
 * Serialize collection objects using MessagePack format specification:
 *
 * https://github.com/msgpack/msgpack/blob/master/spec.md
 */
object JsonPacker {

  @throws(classOf[AerospikeException])
  def pack(v: Json): Array[Byte] = {
    try {
      val packer: JsonPacker = new JsonPacker
      packer.pack(v)
    } catch {
      case e: Exception => throw new AerospikeException.Serialize(e)
    }
  }

}

final class JsonPacker {

  import java.nio.charset.StandardCharsets.UTF_8
  import scala.collection.mutable.ListBuffer

  def pack(doc: Json): Array[Byte] = {
    val buffer: ListBuffer[Byte] = ListBuffer.empty
    go(doc.hcursor, buffer)
    buffer.toArray
  }

  private[this] def go(hc: HCursor, buffer: ListBuffer[Byte]): Unit = {
    hc.focus match {
      case js if js.isArray =>
        js.asArray match {
          case None => // TODO: error?
          case Some(arr) =>
            buffer ++= beginArray(arr.size)
            arr.foreach(e => go(e.hcursor, buffer))
        }
      case js if js.isObject => //
        js.asObject match {
          case None => // TODO: error?
          case Some(e) =>
            val l = e.toList
            beginObject(l.size).foreach(buffer += _)
            l.foreach {
              case (k, v) =>
                packString(k).foreach(buffer += _)
                go(v.hcursor, buffer)
            }
        }
      case js if js.isNull => buffer ++= packNull
      case js if js.isBoolean => js.asBoolean match {
        case None => // TODO: error?
        case Some(x) => buffer ++= packBool(x)
      }
      case js if js.isNumber =>
        val num = js.asNumber
        num match {
          case None => // TODO: error?
          case Some(x) =>
            val n = x.toBigDecimal
            // if (!n.isValidLong) throw new Serialize(new Exception("")) // TODO: error msg
            if (n.isWhole())
              buffer ++= packNum(n.toLong)
            else
              buffer ++= packDouble(n.toDouble)
        }
      case js if js.isString =>
        js.asString match {
          case None => // TODO: error?
          case Some(s) => buffer ++= packString(s)
        }
    }
  }

  private[this] def beginArray(size: Int): Array[Byte] =
    if (size < 16)
      Array((0x90 | size).toByte)
    else if (size < 65536)
      Array(0xdc.toByte, (size >>> 8).toByte, (size >>> 0).toByte)
    else
      Array(0xdd.toByte, (size >>> 24).toByte, (size >>> 16).toByte, (size >>> 8).toByte, (size >>> 0).toByte)

  private[this] def beginObject(sz: Int): Array[Byte] =
    if (sz < 16)
      Array((0x80 | sz).toByte)
    else if (sz < 65536)
      Array(0xde.toByte, (sz >>> 8).toByte, (sz >>> 0).toByte)
    else
      Array(0xdf.toByte, (sz >>> 24).toByte, (sz >>> 16).toByte, (sz >>> 8).toByte, (sz >>> 0).toByte)

  private[this] def beginString(sz: Int): Array[Byte] = {
    if (sz < 32)
      Array(0xa0 | sz, ParticleType.STRING)
    else if (sz < 65536)
      Array(0xda, sz >>> 8, sz >>> 0, ParticleType.STRING)
    else
      Array(0xdb, sz >>> 24, sz >>> 16, sz >>> 8, sz >>> 0, ParticleType.STRING)
  } map {
    _.toByte
  }

  private[this] def packNull(): Array[Byte] = Array(0xc0.toByte)

  private[this] def packBool(v: Boolean): Array[Byte] = v match {
    case true => Array(0xc3.toByte)
    case false => Array(0xc2.toByte)
  }

  private[this] def packString(v: String): Array[Byte] =
    beginString(Buffer.estimateSizeUtf8(v) + 1) ++ v.getBytes(UTF_8)

  private[this] def packFloat(v: Float): Array[Byte] = {
    val x = java.lang.Float.floatToIntBits(v)
    Array(0xca, x >>> 24, x >>> 16, x >>> 8, x >>> 0).map(_.toByte)
  }

  private[this] def packDouble(v: Double): Array[Byte] = {
    val x = java.lang.Double.doubleToLongBits(v)
    Array(0xcb, x >>> 56, x >>> 48, x >>> 40, x >>> 32, x >>> 24, x >>> 16, x >>> 8, x >>> 0).map(_.toByte)
  }

  private[this] def packNum(l: Long): Array[Byte] =
    if (l >= 0)
      l match {
        case n if n < 128L => packByte(n.toInt)
        case n if n < 256L => packByte(0xcc, n.toInt)
        case n if n < 65536L => packShort(0xcd, n.toInt);
        case n if n < 4294967296L => packInt(0xce, n.toInt)

        case n => packLong(0xcf, n)
      }
    else
      l match {
        case n if n >= -32L => packByte(0xe0 | (n + 32).toInt);
        case n if n >= Byte.MinValue.toLong => packByte(0xd0, n.toInt)
        case n if n >= Short.MinValue.toLong => packShort(0xd1, n.toInt)
        case n if n >= Int.MinValue.toLong => packInt(0xd2, n.toInt)
        case n => packLong(0xd3, n)
      }

  private[this] def packByte(v: Int): Array[Byte] = Array(v.toByte)

  private[this] def packByte(t: Int, v: Int): Array[Byte] = Array(t, v).map(_.toByte)

  private[this] def packShort(t: Int, v: Int): Array[Byte] =
    Array(t, v >>> 8, v >>> 0).map(_.toByte)

  private[this] def packInt(t: Int, v: Int): Array[Byte] =
    Array(t, v >>> 24, v >>> 16, v >>> 8, v >>> 0).map(_.toByte)

  private[this] def packLong(t: Long, v: Long): Array[Byte] =
    Array(t, v >>> 56, v >>> 48, v >>> 40, v >>> 32, v >>> 24, v >>> 16, v >>> 8, v >>> 0).map(_.toByte)

}
