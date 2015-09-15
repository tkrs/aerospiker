package aerospiker.msgpack

import java.io.IOException
import com.aerospike.client.AerospikeException
import com.aerospike.client.command.Buffer
import com.aerospike.client.command.ParticleType

import io.circe._
import scala.collection.mutable.ListBuffer

// TODO: Improve implement

/**
 * De-serialize collection objects using MessagePack format specification:
 *
 * https://github.com/msgpack/msgpack/blob/master/spec.md
 */
object JsonUnpacker {

  @throws(classOf[AerospikeException])
  def unpackObjectList(buffer: Array[Byte]): Json =
    new JsonUnpacker(buffer, 0, buffer.length).unpackList

  @throws(classOf[AerospikeException])
  def unpackObjectMap(buffer: Array[Byte]): Json =
    new JsonUnpacker(buffer, 0, buffer.length).unpackMap
}

class JsonUnpacker(buffer: Array[Byte], _offset: Int, length: Int) {

  var offset = _offset

  @throws(classOf[AerospikeException])
  final def unpackList: Json = {
    if (length <= 0) Json.empty
    else
      try {
        val typ: Int = buffer(offset) & 0xff
        offset += 1
        if ((typ & 0xf0) == 0x90) {
          val count = typ & 0x0f
          unpackList(count)
        } else if (typ == 0xdc) {
          val count = Buffer.bytesToShort(buffer, offset)
          offset += 2
          unpackList(count)
        } else if (typ == 0xdd) {
          val count = Buffer.bytesToInt(buffer, offset)
          offset += 4
          unpackList(count)
        } else {
          Json.empty
        }
      } catch {
        case e: Exception => throw new AerospikeException.Serialize(e)
      }
  }

  @throws(classOf[IOException])
  @throws(classOf[ClassNotFoundException])
  private def unpackList(count: Int): Json = {
    import scala.collection.mutable.ListBuffer
    val out: ListBuffer[Json] = ListBuffer.empty
    var i: Int = 0
    while (i < count) {
      out += unpackObject
      i += 1
    }
    Json.fromValues(out.toSeq)
  }

  @throws(classOf[AerospikeException])
  final def unpackMap: Json = {
    if (length <= 0) Json.empty
    else
      try {
        val typ: Int = buffer(offset) & 0xff
        offset += 1
        if ((typ & 0xf0) == 0x80) {
          val count = typ & 0x0f
          unpackMap(count)
        } else if (typ == 0xde) {
          val count = Buffer.bytesToShort(buffer, offset)
          offset += 2
          unpackMap(count)
        } else if (typ == 0xdf) {
          val count = Buffer.bytesToInt(buffer, offset)
          offset += 4
          unpackMap(count)
        } else {
          Json.empty
        }
      } catch {
        case e: Exception =>
          throw new AerospikeException.Serialize(e)
      }
  }

  @throws(classOf[IOException])
  @throws(classOf[ClassNotFoundException])
  private def unpackMap(count: Int): Json = {
    val out: ListBuffer[(String, Json)] = ListBuffer.empty
    for (i <- 0 until count) {
      {
        val o = unpackObject
        val k = o.asString match {
          case Some(s) => s
          case None => throw new AerospikeException(s"key: ${o.pretty(Printer.noSpaces)}") // TODO: exception message
        }
        val v = unpackObject
        out += (k -> v)
      }
    }
    Json.fromFields(out)
  }

  @throws(classOf[IOException])
  @throws(classOf[ClassNotFoundException])
  private def unpackBlob(count: Int): Json = {
    val typ: Int = buffer(offset) & 0xff
    offset += 1
    val c = count - 1
    typ match {
      case ParticleType.STRING =>
        val v = Buffer.utf8ToString(buffer, offset, c)
        offset += c
        Json.string(v)
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
  }

  @throws(classOf[IOException])
  @throws(classOf[ClassNotFoundException])
  def unpackObject: Json = {
    val typ: Int = buffer({ offset += 1; offset - 1 }) & 0xff
    typ match {
      case 0xc0 => Json.empty // TODO: null is empty???
      case 0xc3 => Json.bool(true)
      case 0xc2 => Json.bool(false)
      case 0xca =>
        val v: Float = java.lang.Float.intBitsToFloat(Buffer.bytesToInt(buffer, offset))
        offset += 4
        Json.numberOrNull(v.toDouble)
      case 0xcb =>
        val v: Double = java.lang.Double.longBitsToDouble(Buffer.bytesToLong(buffer, offset))
        offset += 8
        Json.numberOrNull(v)
      case 0xd0 =>
        val v = buffer(offset).toLong
        offset += 1
        Json.long(v)
      case 0xcc =>
        val v = buffer(offset & 0xff).toLong
        offset += 1
        Json.long(v)
      case 0xd1 =>
        val v: Short = Buffer.bigSigned16ToShort(buffer, offset)
        offset += 2
        Json.long(v.toLong)
      case 0xcd =>
        val v: Int = Buffer.bytesToShort(buffer, offset)
        offset += 2
        Json.long(v.toLong)
      case 0xd2 =>
        val v: Int = Buffer.bytesToInt(buffer, offset)
        offset += 4
        Json.long(v.toLong)
      case 0xce =>
        val v: Long = Buffer.bigUnsigned32ToLong(buffer, offset)
        offset += 4
        Json.long(v)
      case 0xd3 =>
        val v: Long = Buffer.bytesToLong(buffer, offset)
        offset += 8
        Json.long(v)
      case 0xcf =>
        val v: Long = Buffer.bytesToLong(buffer, offset)
        offset += 8
        Json.long(v)
      case 0xda =>
        val count: Int = Buffer.bytesToShort(buffer, offset)
        offset += 2
        unpackBlob(count) // String only
      case 0xdb =>
        val count: Int = Buffer.bytesToInt(buffer, offset)
        offset += 4
        unpackBlob(count) // String only
      case 0xdc =>
        val count: Int = Buffer.bytesToShort(buffer, offset)
        offset += 2
        unpackList(count)
      case 0xdd =>
        val count: Int = Buffer.bytesToInt(buffer, offset)
        offset += 4
        unpackList(count)
      case 0xde =>
        val count: Int = Buffer.bytesToShort(buffer, offset)
        offset += 2
        unpackMap(count)
      case 0xdf =>
        val count: Int = Buffer.bytesToInt(buffer, offset)
        offset += 4
        unpackMap(count)
      case _ =>
        if ((typ & 0xe0) == 0xa0) unpackBlob(typ & 0x1f)
        else if ((typ & 0xf0) == 0x80) unpackMap(typ & 0x0f)
        else if ((typ & 0xf0) == 0x90) unpackList(typ & 0x0f)
        else if (typ < 0x80) Json.long(typ.toLong)
        else if (typ >= 0xe0) Json.long((typ - 0xe0 - 32).toLong)
        else throw new IOException("Unknown unpack type: " + typ)
    }
  }
}
