package aerospiker

import java.nio.ByteBuffer
import com.aerospike.client.policy._
import com.aerospike.client.{ AerospikeException, Operation }
import com.aerospike.client.async.{ AsyncSingleCommand, AsyncCluster, AsyncNode }
import com.aerospike.client.cluster.Partition
import com.aerospike.client.command.{ Command => C, ParticleType, FieldType }
import aerospiker.policy.{ Policy, WritePolicy }

import aerospiker.msgpack.JsonPacker
import com.typesafe.scalalogging.LazyLogging
import io.circe._
import io.circe.syntax._

import scala.collection.mutable.ListBuffer

// TODO: improve implement

final class AsyncWrite[T](
    cluster: AsyncCluster,
    policy: WritePolicy,
    listener: Option[WriteListener],
    key: Key,
    bins: T,
    operation: Operation.Type
)(
    implicit
    encoder: Encoder[T]
) extends AsyncSingleCommand(cluster) with LazyLogging {

  val partition = new Partition(key)

  def getPolicy: Policy = policy

  def writeBuffer(): Unit = {
    val start = System.currentTimeMillis()
    val buffer = setWrite(policy, operation, key, bins)
    val end = System.currentTimeMillis()

    sizeBuffer()
    System.arraycopy(buffer, 0, dataBuffer, 0, buffer.length)
    dataOffset = buffer.length
  }

  def getNode: AsyncNode = cluster.getMasterNode(partition).asInstanceOf[AsyncNode]

  def parseResult(byteBuffer: ByteBuffer): Unit = {
    val resultCode: Int = byteBuffer.get(5) & 0xFF
    if (resultCode != 0) throw new AerospikeException(resultCode)
  }

  def onSuccess(): Unit = listener match {
    case None => // nop
    case Some(l) => l.onSuccess(key)
  }

  def onFailure(e: AerospikeException): Unit = listener match {
    case None =>
    case Some(l) => l.onFailure(e)
  }

  @throws(classOf[AerospikeException])
  def setWrite(policy: WritePolicy, operation: Operation.Type, key: Key, bins: T): Array[Byte] = {

    val byteBuffer: ListBuffer[Byte] = ListBuffer.empty

    val json = bins.asJson
    val jsonObjs = json.asObject.getOrElse(JsonObject.empty).toList

    val fields = Seq(
      writeField(key.namespace, FieldType.NAMESPACE, 0, true),
      writeField(key.setName, FieldType.TABLE, 0, true),
      writeField(key.digest, FieldType.DIGEST_RIPE, 0, true),
      writeField(key.userKey.toString, FieldType.KEY, 1, policy.sendKey)
    ) collect { case Some(a) => a }

    val header = (updateHeader3 andThen updateHeader2 andThen updateHeader1 andThen updateHeader0)(
      Header(headerLength = C.MSG_REMAINING_HEADER_SIZE, writeAttr = C.INFO2_WRITE, fieldCount = fields.size, operationCount = jsonObjs.size)
    )

    byteBuffer ++= header.getBytes

    // Write key into buffer.
    byteBuffer ++= fields.flatten

    val values = jsonObjs.foreach {
      case (k: String, v: Json) =>
        val (nameBytes, nameLength) = Buffer.stringToUtf8(k)
        val (valueBytes, valueLength, particleType) = writeValue(v) // bin.value.write(byteBuffer, dataOffset + OPERATION_HEADER_SIZE + nameLength)
        byteBuffer ++= Buffer.intToBytes(nameLength + valueLength + 4)
        byteBuffer += operation.protocolType.toByte
        byteBuffer += particleType.toByte
        byteBuffer += 0.toByte
        byteBuffer += nameLength.toByte
        byteBuffer ++= nameBytes
        byteBuffer ++= valueBytes
    }

    val size: Long = byteBuffer.size | (C.CL_MSG_VERSION << 56) | (C.AS_MSG_TYPE << 48)
    byteBuffer prependAll Buffer.longToBytes(size)

    byteBuffer.toArray
  }

  val updateHeader0: (Header) => (Header) = { h =>
    import RecordExistsAction._
    policy.recordExistsAction match {
      case UPDATE => h
      case UPDATE_ONLY => h.withInfoAttr(h.infoAttr | C.INFO3_UPDATE_ONLY)
      case REPLACE => h.withInfoAttr(h.infoAttr | C.INFO3_CREATE_OR_REPLACE)
      case REPLACE_ONLY => h.withInfoAttr(h.infoAttr | C.INFO3_REPLACE_ONLY)
      case CREATE_ONLY => h.withWriteAttr(h.writeAttr | C.INFO2_CREATE_ONLY)
      case _ => h
    }
  }

  val updateHeader1: (Header) => (Header) = { h =>
    import GenerationPolicy._
    policy.generationPolicy match {
      case NONE => h
      case EXPECT_GEN_EQUAL =>
        h.withGeneration(policy.generation).withWriteAttr(h.writeAttr | C.INFO2_GENERATION)
      case EXPECT_GEN_GT =>
        h.withGeneration(policy.generation).withWriteAttr(h.writeAttr | C.INFO2_GENERATION_GT)
      case _ => h
    }
  }

  val updateHeader2: (Header) => (Header) = { h =>
    import CommitLevel._
    policy.commitLevel match {
      case COMMIT_MASTER =>
        h.withInfoAttr(h.infoAttr | C.INFO3_COMMIT_MASTER)
      case _ => h
    }
  }

  val updateHeader3: (Header) => (Header) = { h =>
    import ConsistencyLevel._
    policy.consistencyLevel match {
      case CONSISTENCY_ALL =>
        h.withReadAttr(h.readAttr | C.INFO1_CONSISTENCY_ALL)
      case _ => h
    }
  }

  def writeField(str: String, typ: Int, add: Int, send: Boolean): Option[Seq[Byte]] = {
    if (!send) None
    else {
      val (fieldBytes, fieldLength) = Buffer.stringToUtf8(str)
      val lenBytes = Buffer.intToBytes(fieldLength + 1 + add)
      val b = lenBytes ++ Array(typ.toByte)
      if (typ == FieldType.KEY) {
        Some(b ++ Array(ParticleType.STRING.toByte) ++ fieldBytes)
      } else {
        Some(b ++ fieldBytes)
      }
    }
  }

  def writeField(bytes: Array[Byte], typ: Int, add: Int, send: Boolean): Option[Seq[Byte]] =
    if (!send) None
    else {
      val lenBytes = Buffer.intToBytes(bytes.length + 1 + add)
      val b = lenBytes ++ Array(typ.toByte) ++ bytes
      Some(b)
    }

  @throws(classOf[AerospikeException])
  private def writeValue(value: Json): (Array[Byte], Int, Int) = value match {
    case js if js.isObject =>
      val bytes = JsonPacker.pack(value)
      (bytes, bytes.length, ParticleType.MAP)
    case js if js.isArray =>
      val bytes = JsonPacker.pack(value)
      (bytes, bytes.length, ParticleType.LIST)
    case js if js.isString =>
      val (bytes, len) = Buffer.stringToUtf8(js.asString.getOrElse(""))
      (bytes, len, ParticleType.STRING)
    case js if js.isNumber => js.asNumber match {
      case None => throw new AerospikeException("number parse error")
      case Some(jo) => jo.toBigDecimal match {
        case d if d.isWhole() =>
          val bytes = Buffer.longToBytes(d.longValue())
          (bytes.toArray, bytes.length, ParticleType.INTEGER)
        case d =>
          val bytes = doubleToBytes(d.doubleValue())
          (bytes.toArray, bytes.length, ParticleType.DOUBLE)
      }
    }
    case js if js.isNull => (Array.empty, 0, ParticleType.NULL)
    case js if js.isBoolean => js.asBoolean.getOrElse(false) match {
      case true => (Buffer.longToBytes(1L).toArray, 8, ParticleType.INTEGER)
      case false => (Buffer.longToBytes(0L).toArray, 8, ParticleType.INTEGER)
    }
    case _ => throw new AerospikeException(s"Unsuported object [${value.pretty(Printer.noSpaces)}]")
  }

  def doubleToBytes(v: Double): Array[Byte] = {
    Buffer.longToBytes(java.lang.Double.doubleToLongBits(v)).toArray
  }
}

case class Header(
    headerLength: Int = 0,
    readAttr: Int = 0,
    writeAttr: Int = 0,
    infoAttr: Int = 0,
    resultCode: Int = 0,
    generation: Int = 0,
    expiration: Int = 0,
    timeOut: Int = 0,
    fieldCount: Int = 0,
    operationCount: Int = 0
) {
  def withInfoAttr(infoAttr: Int) = copy(infoAttr = infoAttr)
  def withReadAttr(readAttr: Int) = copy(readAttr = readAttr)
  def withWriteAttr(writeAttr: Int) = copy(writeAttr = writeAttr)
  def withGeneration(generation: Int) = copy(generation = generation)
  def getBytes: Seq[Byte] = {
    // Write all header data except total size which must be written last.
    val bytes: ListBuffer[Byte] = ListBuffer.empty
    bytes += headerLength.toByte // Message header length.
    bytes += readAttr.toByte
    bytes += writeAttr.toByte
    bytes += infoAttr.toByte
    bytes += 0.toByte //
    bytes += 0.toByte // clear the result code
    bytes ++= Buffer.intToBytes(generation)
    bytes ++= Buffer.intToBytes(expiration)
    // Initialize timeout. It will be written later.
    bytes ++= (0 :: 0 :: 0 :: 0 :: Nil).map(_.toByte)
    bytes ++= Buffer.shortToBytes(fieldCount.toShort)
    bytes ++= Buffer.shortToBytes(operationCount.toShort)
    bytes.toSeq
  }

}

