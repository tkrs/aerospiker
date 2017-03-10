package aerospiker
package command

import java.nio.ByteBuffer

import aerospiker.buffer.{ Buffer, Header }
import aerospiker.listener.WriteListener
import aerospiker.msgpack.JsonPacker
import aerospiker.policy.{ Policy, WritePolicy }
import com.aerospike.client.async.{ AsyncCluster, AsyncCommand, AsyncSingleCommand }
import com.aerospike.client.cluster.{ Node, Partition }
import com.aerospike.client.command.{ FieldType, ParticleType, Command => C }
import com.aerospike.client.policy._
import com.aerospike.client.{ AerospikeException, Operation }
import io.circe._
import io.circe.syntax._

import scala.collection.mutable.ListBuffer

final class Write[T: Encoder](
    cluster: AsyncCluster,
    policy: WritePolicy,
    listener: Option[WriteListener],
    key: Key,
    bins: T,
    operation: Operation.Type
) extends AsyncSingleCommand(cluster, policy) {

  val partition = new Partition(key)

  def getPolicy: Policy = policy

  def writeBuffer(): Unit = {
    val buffer = setWrite(policy, operation, key, bins)
    sizeBuffer()
    System.arraycopy(buffer, 0, dataBuffer, 0, buffer.length)
    dataOffset = buffer.length
  }

  def getNode: Node = cluster.getMasterNode(partition)

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
      writeField(key.namespace, FieldType.NAMESPACE, 0, java.lang.Boolean.TRUE),
      writeField(key.setName, FieldType.TABLE, 0, java.lang.Boolean.TRUE),
      writeField(key.digest, FieldType.DIGEST_RIPE, 0, java.lang.Boolean.TRUE),
      writeField(key.userKey.toString, FieldType.KEY, 1, policy.sendKey)
    ) collect { case Some(a) => a }

    val header = (updateHeader3 andThen updateHeader2 andThen updateHeader1 andThen updateHeader0)(
      Header(headerLength = C.MSG_REMAINING_HEADER_SIZE, writeAttr = C.INFO2_WRITE, fieldCount = fields.size, operationCount = jsonObjs.size)
    )

    byteBuffer ++= header.getBytes

    // Write key into buffer.
    byteBuffer ++= fields.flatten

    jsonObjs.foreach {
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

  private[this] val packer = JsonPacker()

  @throws(classOf[AerospikeException])
  private def writeValue(value: Json): (Array[Byte], Int, Int) = value match {
    case js if js.isObject =>
      val bytes = packer.pack(value) match {
        case Left(e) => throw e
        case Right(arr) => arr
      }
      (bytes, bytes.length, ParticleType.MAP)
    case js if js.isArray =>
      val bytes = packer.pack(value) match {
        case Left(e) => throw e
        case Right(arr) => arr
      }
      (bytes, bytes.length, ParticleType.LIST)
    case js if js.isString =>
      val (bytes, len) = Buffer.stringToUtf8(js.asString.getOrElse(""))
      (bytes, len, ParticleType.STRING)
    case js if js.isNumber => js.asNumber match {
      case None => throw new AerospikeException("number parse error")
      case Some(jo) => jo.toBigDecimal match {
        case None => throw new AerospikeException("number parse error")
        case Some(d) if JsonPacker.double(d) =>
          val bytes = doubleToBytes(d.doubleValue())
          (bytes, bytes.length, ParticleType.DOUBLE)
        case Some(d) =>
          val bytes = Buffer.longToBytes(d.longValue())
          (bytes.toArray, bytes.length, ParticleType.INTEGER)
      }
    }
    case js if js.isNull => (Array.empty, 0, ParticleType.NULL)
    case js if js.isBoolean =>
      if (js.asBoolean.getOrElse(false)) (Buffer.longToBytes(1L).toArray, 8, ParticleType.INTEGER)
      else (Buffer.longToBytes(0L).toArray, 8, ParticleType.INTEGER)
    case _ => throw new AerospikeException(s"Unsupported object [${value.pretty(Printer.noSpaces)}]")
  }

  def doubleToBytes(v: Double): Array[Byte] =
    Buffer.longToBytes(java.lang.Double.doubleToLongBits(v)).toArray

  override def cloneCommand(): AsyncCommand = new Write[T](cluster, policy, listener, key, bins, operation)
}