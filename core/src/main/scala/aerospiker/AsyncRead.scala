package aerospiker

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

import com.aerospike.client.AerospikeException
import com.aerospike.client.AerospikeException.Parse
import com.aerospike.client.ResultCode
import com.aerospike.client.async.{ AsyncNode, AsyncCluster }

import com.aerospike.client.cluster.Partition
import com.aerospike.client.command.Command
import com.aerospike.client.policy.Policy
import com.aerospike.client.util.ThreadLocalData
import cats.data.Xor._
import com.typesafe.scalalogging.LazyLogging

import io.circe._

import scala.collection.mutable.ListBuffer

class AsyncRead[T](
    cluster: AsyncCluster,
    policy: Policy,
    listener: Option[RecordListener[T]],
    key: Key,
    binNames: String*
)(
    implicit
    decoder: Decoder[T]
) extends com.aerospike.client.async.AsyncSingleCommand(cluster) with LazyLogging {

  val partition = new Partition(key)
  var record: Option[Record[T]] = None

  def getPolicy = policy

  def writeBuffer(): Unit = {
    val start = System.currentTimeMillis()
    setRead(policy, key, if (binNames.isEmpty) null else binNames.toArray)
    val end = System.currentTimeMillis()
    logger.debug(s"${end - start} ms")
  }

  def getNode: AsyncNode = cluster.getReadNode(partition, policy.replica).asInstanceOf[AsyncNode]

  def parseResult(byteBuffer: ByteBuffer): Unit = {
    dataBuffer = ThreadLocalData.getBuffer
    if (receiveSize > dataBuffer.length) {
      dataBuffer = ThreadLocalData.resizeBuffer(receiveSize)
    }
    logger.debug(s"parseResult start :: dataOffset => [$dataOffset], receiveSize => [$receiveSize]")

    // Copy entire message to dataBuffer.
    byteBuffer.position(0)
    byteBuffer.get(dataBuffer, 0, receiveSize)

    val resultCode = dataBuffer(5) & 0xFF
    val generation = Buffer.bytesToInt(dataBuffer.slice(6, 10))
    val expiration = Buffer.bytesToInt(dataBuffer.slice(10, 14))
    val fieldCount = Buffer.bytesToShort(dataBuffer.slice(18, 20))
    val opCount = Buffer.bytesToShort(dataBuffer.slice(20, 22))
    dataOffset = Command.MSG_REMAINING_HEADER_SIZE
    logger.debug(s"resultCode[$resultCode] generation[$generation] expiration[$expiration] fieldCount[$fieldCount] opCount[$opCount]")

    if (resultCode == 0) {
      if (opCount == 0) {
        // Bin data was not returned.
        record = Some(Record(None, generation, expiration))
      } else {
        record = Some(parseRecord(opCount.toInt, fieldCount.toInt, generation, expiration))
      }
    } else {
      if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
        record = None
      } else {
        throw new AerospikeException(resultCode)
      }
    }
  }

  def parseRecord(opCount: Int, fieldCount: Int, generation: Int, expiration: Int): Record[T] = {
    // There can be fields in the response (setname etc).
    // But for now, ignore them. Expose them to the API if needed in the future.
    if (fieldCount > 0) {
      // Just skip over all the fields
      for (i <- 0 until fieldCount) {
        val fieldSize = Buffer.bytesToInt(dataBuffer.slice(dataOffset, dataOffset + 4))
        dataOffset += 4 + fieldSize
      }
    }
    val bins: ListBuffer[(String, Json)] = ListBuffer.empty
    for (i <- 0 until opCount) {
      logger.debug(s"dataOffset[$dataOffset]-------------------------------------------")
      val opSize = Buffer.bytesToInt(dataBuffer.slice(dataOffset, dataOffset + 4))
      logger.debug(s"opSize[$opSize]")
      val particleType = dataBuffer(dataOffset + 5).toInt
      logger.debug(s"particleType[$particleType]")
      val nameSize = dataBuffer(dataOffset + 7).toInt
      logger.debug(s"nameSize[$nameSize]")
      // val name = Buffer.utf8ToString(dataBuffer, dataOffset + 8, nameSize)
      val name = new String(dataBuffer.slice(dataOffset + 8, dataOffset + 8 + nameSize), UTF_8)
      logger.debug(s"name[$name]")
      dataOffset += 4 + 4 + nameSize
      val particleBytesSize = opSize - (4 + nameSize)
      logger.debug(s"particleBytesSize[$particleBytesSize]")
      val result = Buffer.bytesToParticle(particleType, dataBuffer.slice(dataOffset, dataOffset + particleBytesSize))
      logger.debug(s"result[$result]")
      bins += (name -> result)
      dataOffset += particleBytesSize
    }
    val doc = Json.obj(bins: _*)
    logger.debug(doc.pretty(Printer.noSpaces))
    doc.as[T] match {
      case Left(e) => throw new Parse(e.getMessage())
      case Right(v) => Record(Some(v), generation, expiration)
    }

  }

  def onSuccess(): Unit = listener match {
    case Some(l) => l.onSuccess(key, record)
    case None => // nop
  }

  def onFailure(e: AerospikeException): Unit = listener match {
    case Some(l) => l.onFailure(e)
    case None => // nop
  }
}
