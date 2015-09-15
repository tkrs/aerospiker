package aerospiker

// TODO: hate!

import aerospiker.policy.{ Policy, ScanPolicy }
import com.aerospike.client.{ Operation, AerospikeException }
import com.aerospike.client.AerospikeException.Parse
import com.aerospike.client.async.{ AsyncNode, AsyncCluster, AsyncMultiExecutor, AsyncMultiCommand }
import com.aerospike.client.command.{ Command, FieldType }
import com.typesafe.scalalogging.LazyLogging
import io.circe.{ Printer, Decoder, Json }

import cats.data.Xor._

import scala.collection.mutable.ListBuffer

final class AsyncScan[T](
    parent: AsyncMultiExecutor,
    cluster: AsyncCluster,
    node: AsyncNode,
    policy: ScanPolicy,
    listener: Option[RecordSequenceListener[T]],
    namespace: String,
    setName: String,
    binNames: Array[String],
    taskId: Long
)(
    implicit
    decoder: Decoder[T]
) extends AsyncMultiCommand(parent, cluster, node, true) with LazyLogging {

  def getPolicy: Policy = policy

  @throws(classOf[AerospikeException])
  def writeBuffer(): Unit = setScan(policy, namespace, setName, binNames, taskId)

  //  def setScan(policy: ScanPolicy, namespace: String, setName: String, binNames: Array[String], taskId: Long) {
  //    begin
  //    var fieldCount: Int = 0
  //    if (namespace != null) {
  //      dataOffset += Buffer.estimateSizeUtf8(namespace) + Command.FIELD_HEADER_SIZE
  //      fieldCount += 1
  //    }
  //    if (setName != null) {
  //      dataOffset += Buffer.estimateSizeUtf8(setName) + Command.FIELD_HEADER_SIZE
  //      fieldCount += 1
  //    }
  //    dataOffset += 2 + Command.FIELD_HEADER_SIZE
  //    fieldCount += 1
  //    dataOffset += 8 + Command.FIELD_HEADER_SIZE
  //    fieldCount += 1
  //    if (binNames != null) {
  //      for (binName <- binNames) {
  //        estimateOperationSize(binName)
  //      }
  //    }
  //    sizeBuffer
  //    var readAttr: Byte = Command.INFO1_READ.toByte
  //    if (!policy.includeBinData) {
  //      readAttr |= Command.INFO1_NOBINDATA
  //    }
  //    val operationCount: Int = if ((binNames == null)) 0 else binNames.length
  //    writeHeader(policy, readAttr, 0, fieldCount, operationCount)
  //    if (namespace != null) {
  //      writeField(namespace, FieldType.NAMESPACE)
  //    }
  //    if (setName != null) {
  //      writeField(setName, FieldType.TABLE)
  //    }
  //    writeFieldHeader(2, FieldType.SCAN_OPTIONS)
  //    var priority: Byte = policy.priority.ordinal.toByte
  //    priority <<= 4
  //    if (policy.failOnClusterChange) {
  //      priority |= 0x08
  //    }
  //    dataBuffer(({
  //      dataOffset += 1; dataOffset - 1
  //    })) = priority
  //    dataBuffer(({
  //      dataOffset += 1; dataOffset - 1
  //    })) = policy.scanPercent.toByte
  //    writeFieldHeader(8, FieldType.TRAN_ID)
  //    Buffer.longToBytes(taskId, dataBuffer, dataOffset)
  //    dataOffset += 8
  //    if (binNames != null) {
  //      for (binName <- binNames) {
  //        writeOperation(binName, Operation.Type.READ)
  //      }
  //    }
  //    end
  //  }

  @throws(classOf[AerospikeException])
  def parseRow(key: Key): Unit = {
    logger.debug(s"key[$key]")
    val start = System.currentTimeMillis()
    val record: Option[Record[T]] = parseRecord0()
    listener match {
      case Some(l) => l.onRecord(key, record)
      case None => // nop
    }
    val end = System.currentTimeMillis()
    logger.debug(s"${end - start} ms")
  }

  @throws(classOf[AerospikeException])
  def parseRecord0(): Option[Record[T]] = {
    logger.debug(s"= parseRecord0 opCount[$opCount] ==========")
    val bins: ListBuffer[(String, Json)] = ListBuffer.empty
    for (i <- 0 until opCount) {
      logger.debug("----------------")
      val opSize: Int = Buffer.bytesToInt(receiveBuffer.slice(receiveOffset, receiveOffset + 4))
      logger.debug(s"opSize[$opSize]")
      val particleType: Byte = receiveBuffer(receiveOffset + 5)
      logger.debug(s"particleType[$particleType]")
      val nameSize: Byte = receiveBuffer(receiveOffset + 7)
      logger.debug(s"nameSize[$nameSize]")
      val name: String = new String(receiveBuffer.slice(receiveOffset + 8, receiveOffset + 8 + nameSize))
      logger.debug(s"name[$name]")
      receiveOffset += 4 + 4 + nameSize
      val particleBytesSize: Int = opSize - (4 + nameSize)
      logger.debug(s"particleBytesSize[$particleBytesSize]")
      val value: Json = Buffer.bytesToParticle(particleType.toInt, receiveBuffer.slice(receiveOffset, receiveOffset + particleBytesSize))
      logger.debug(s"value[${value.pretty(Printer.noSpaces)}]")
      receiveOffset += particleBytesSize

      bins += name -> value
      logger.debug(bins.map(e => (e._1, e._2.pretty(Printer.noSpaces))).toString)
    }
    val json = Json.obj(bins: _*)
    logger.debug(s"json[${json.pretty(Printer.noSpaces)}]")
    json.as[T] match {
      case Left(e) => throw new Parse(e.getMessage)
      case Right(t) => Some(Record[T](Some(t), generation, expiration))
    }

  }
}

