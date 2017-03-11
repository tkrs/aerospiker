package aerospiker

import com.aerospike.client.{ AerospikeException, Operation }
import com.aerospike.client.command.{ FieldType, ParticleType, Command => C }
import com.aerospike.client.policy._
import io.circe.{ Encoder, Json, JsonObject }
import io.circe.syntax._

import scala.collection.mutable

package object protocol {

  @throws(classOf[AerospikeException])
  def setWrite[T: Encoder](policy: WritePolicy, operation: Operation.Type, key: Key, bins: T): Array[Byte] = {
    val byteBuffer: mutable.ArrayBuilder[Byte] = mutable.ArrayBuilder.make()
    val json = bins.asJson
    val jsonObjs = json.asObject.getOrElse(JsonObject.empty).toList
    val fields = Seq(
      writeField(key.namespace, FieldType.NAMESPACE, 0, java.lang.Boolean.TRUE),
      writeField(key.setName, FieldType.TABLE, 0, java.lang.Boolean.TRUE),
      writeField(key.digest, FieldType.DIGEST_RIPE, 0, java.lang.Boolean.TRUE),
      writeField(key.userKey.toString, FieldType.KEY, 1, policy.sendKey)
    ) collect { case Some(a) => a }
    val header = (updateHeader3(policy) andThen updateHeader2(policy) andThen updateHeader1(policy) andThen updateHeader0(policy))(
      Header(
        headerLength = C.MSG_REMAINING_HEADER_SIZE,
        writeAttr = C.INFO2_WRITE,
        fieldCount = fields.size,
        operationCount = jsonObjs.size
      )
    )
    byteBuffer ++= header.getBytes
    // Write key into buffer.
    byteBuffer ++= fields.flatten
    jsonObjs.foreach {
      case (k: String, v: Json) =>
        val nameBytes = Buffer.stringToUtf8(k)
        val nameLength = nameBytes.length
        val (bs, pt) = writeValue(v)
        byteBuffer ++= Buffer.intToBytes(nameLength + bs.length + 4)
        byteBuffer += operation.protocolType.toByte
        byteBuffer += pt.toByte
        byteBuffer += 0.toByte
        byteBuffer += nameLength.toByte
        byteBuffer ++= nameBytes
        byteBuffer ++= bs
    }
    val arr = byteBuffer.result()
    val size: Long = arr.length | (C.CL_MSG_VERSION << 56) | (C.AS_MSG_TYPE << 48)
    Buffer.longToBytes(size) ++ arr
  }

  private[this] def writeField(str: String, typ: Int, add: Int, send: Boolean): Option[Seq[Byte]] =
    if (!send) None else {
      val fieldBytes = Buffer.stringToUtf8(str)
      val fieldLength = fieldBytes.length
      val lenBytes = Buffer.intToBytes(fieldLength + 1 + add)
      val b = lenBytes ++ Array(typ.toByte)
      if (typ == FieldType.KEY) {
        Some(b ++ Array(ParticleType.STRING.toByte) ++ fieldBytes)
      } else {
        Some(b ++ fieldBytes)
      }
    }

  private[this] def writeField(bytes: Array[Byte], typ: Int, add: Int, send: Boolean): Option[Seq[Byte]] =
    if (!send) None
    else Some(Buffer.intToBytes(bytes.length + 1 + add) ++ Array(typ.toByte) ++ bytes)

  private[this] val updateHeader0: WritePolicy => Header => Header = p => h => p.recordExistsAction match {
    case RecordExistsAction.UPDATE => h
    case RecordExistsAction.UPDATE_ONLY => h.withInfoAttr(h.infoAttr | C.INFO3_UPDATE_ONLY)
    case RecordExistsAction.REPLACE => h.withInfoAttr(h.infoAttr | C.INFO3_CREATE_OR_REPLACE)
    case RecordExistsAction.REPLACE_ONLY => h.withInfoAttr(h.infoAttr | C.INFO3_REPLACE_ONLY)
    case RecordExistsAction.CREATE_ONLY => h.withWriteAttr(h.writeAttr | C.INFO2_CREATE_ONLY)
    case _ => h
  }

  private[this] val updateHeader1: WritePolicy => Header => Header = p => h => p.generationPolicy match {
    case GenerationPolicy.NONE => h
    case GenerationPolicy.EXPECT_GEN_EQUAL => h.withGeneration(p.generation).withWriteAttr(h.writeAttr | C.INFO2_GENERATION)
    case GenerationPolicy.EXPECT_GEN_GT => h.withGeneration(p.generation).withWriteAttr(h.writeAttr | C.INFO2_GENERATION_GT)
    case _ => h
  }

  private[this] val updateHeader2: WritePolicy => Header => Header = p => h => p.commitLevel match {
    case CommitLevel.COMMIT_MASTER => h.withInfoAttr(h.infoAttr | C.INFO3_COMMIT_MASTER)
    case _ => h
  }

  private[this] val updateHeader3: WritePolicy => Header => Header = p => h => p.consistencyLevel match {
    case ConsistencyLevel.CONSISTENCY_ALL => h.withReadAttr(h.readAttr | C.INFO1_CONSISTENCY_ALL)
    case _ => h
  }

  @throws(classOf[AerospikeException])
  private[this] def writeValue(value: Json): (Array[Byte], Int) = value match {
    case js if js.isObject => (packer.pack(value).fold[Array[Byte]](throw _, identity), ParticleType.MAP)
    case js if js.isArray => (packer.pack(value).fold[Array[Byte]](throw _, identity), ParticleType.LIST)
    case js if js.isString => (Buffer.stringToUtf8(js.asString.getOrElse("")), ParticleType.STRING)
    case js if js.isNumber => js.asNumber match {
      case None => throw new AerospikeException("number parse error")
      case Some(jo) => jo.toBigDecimal match {
        case None => throw new AerospikeException("number parse error")
        case Some(d) if JsonPacker.double(d) =>
          val bytes = doubleToBytes(d.doubleValue())
          (bytes, ParticleType.DOUBLE)
        case Some(d) =>
          val bytes = Buffer.longToBytes(d.longValue())
          (bytes, ParticleType.INTEGER)
      }
    }
    case js if js.isNull => (Array.empty, ParticleType.NULL)
    case js if js.isBoolean =>
      if (js.asBoolean.getOrElse(false)) (Buffer.longToBytes(1L), ParticleType.INTEGER)
      else (Buffer.longToBytes(0L), ParticleType.INTEGER)
    case _ => throw new AerospikeException(s"Unsupported object [${value.noSpaces}]")
  }

  private[this] def doubleToBytes(v: Double): Array[Byte] = Buffer.longToBytes(java.lang.Double.doubleToLongBits(v))
}
