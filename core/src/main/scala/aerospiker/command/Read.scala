package aerospiker
package command

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

import aerospiker.protocol.Buffer
import aerospiker.listener.RecordListener
import aerospiker.policy.Policy
import com.aerospike.client.AerospikeException
import com.aerospike.client.AerospikeException.Parse
import com.aerospike.client.ResultCode._
import com.aerospike.client.async.{ AsyncCluster, AsyncCommand, AsyncSingleCommand }
import com.aerospike.client.cluster.{ Node, Partition }
import com.aerospike.client.command.Command
import com.aerospike.client.util.ThreadLocalData
import io.circe.{ Decoder, Json }

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

class Read[T: Decoder](
    cluster: AsyncCluster,
    policy: Policy,
    listener: Option[RecordListener[T]],
    key: Key,
    binNames: String*
) extends AsyncSingleCommand(cluster, policy) {

  val partition = new Partition(key)
  var record: Option[Record[T]] = None

  override def writeBuffer(): Unit =
    setRead(policy, key, if (binNames.isEmpty) null else binNames.toArray)

  override def getNode: Node = cluster.getReadNode(partition, policy.replica)

  override def parseResult(byteBuffer: ByteBuffer): Unit = {
    dataBuffer = ThreadLocalData.getBuffer
    if (receiveSize > dataBuffer.length)
      dataBuffer = ThreadLocalData.resizeBuffer(receiveSize)

    // Copy entire message to dataBuffer.
    byteBuffer.position(0)
    byteBuffer.get(dataBuffer, 0, receiveSize)

    val resultCode = dataBuffer(5) & 0xFF
    val generation = Buffer.bytesToInt(dataBuffer.slice(6, 10))
    val expiration = Buffer.bytesToInt(dataBuffer.slice(10, 14))
    val fieldCount = Buffer.bytesToShort(dataBuffer.slice(18, 20))
    val opCount = Buffer.bytesToShort(dataBuffer.slice(20, 22))
    dataOffset = Command.MSG_REMAINING_HEADER_SIZE

    if (resultCode == 0) {
      if (opCount == 0) {
        // Bin data was not returned.
        record = Some(Record(None, generation, expiration))
      } else {
        record = Some(parseRecord(opCount.toInt, fieldCount.toInt, generation, expiration))
      }
    } else {
      if (resultCode == KEY_NOT_FOUND_ERROR) {
        record = None
      } else {
        throw new AerospikeException(resultCode)
      }
    }
  }

  private[this] def parseRecord(opCount: Int, fieldCount: Int, generation: Int, expiration: Int): Record[T] = {
    for (_ <- 0 until fieldCount) {
      val fieldSize = Buffer.bytesToInt(dataBuffer.slice(dataOffset, dataOffset + 4))
      dataOffset += 4 + fieldSize
    }

    @tailrec def go(i: Int, acc: ListBuffer[(String, Json)]): Record[T] = i match {
      case 0 =>
        Json.obj(acc: _*).as[T] match {
          case Left(e) => throw new Parse(e.getMessage())
          case Right(v) => Record(Some(v), generation, expiration)
        }
      case _ =>
        val opSize = Buffer.bytesToInt(dataBuffer.slice(dataOffset, dataOffset + 4))
        val particleType = dataBuffer(dataOffset + 5).toInt
        val nameSize = dataBuffer(dataOffset + 7).toInt
        val name = new String(dataBuffer.slice(dataOffset + 8, dataOffset + 8 + nameSize), UTF_8)
        dataOffset += 4 + 4 + nameSize
        val particleBytesSize = opSize - (4 + nameSize)
        val result = Buffer.bytesToParticle(particleType, dataBuffer.slice(dataOffset, dataOffset + particleBytesSize))
        acc += (name -> result)
        dataOffset += particleBytesSize
        go(i - 1, acc)
    }
    go(opCount, ListBuffer.empty)
  }

  override def onSuccess(): Unit = listener.foreach(_.onSuccess(key, record))

  override def onFailure(e: AerospikeException): Unit = listener.foreach(_.onFailure(e))

  override def cloneCommand(): AsyncCommand = new Read[T](cluster, policy, listener, key, binNames: _*)
}
