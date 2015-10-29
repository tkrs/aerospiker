package aerospiker
package command

import java.nio.ByteBuffer

import aerospiker.data._
import com.aerospike.client.{ AerospikeException, ResultCode }
import com.aerospike.client.async.{ AsyncCluster, AsyncNode, AsyncSingleCommand }
import com.aerospike.client.cluster.Partition
import com.aerospike.client.policy.Policy
import io.circe.Decoder

import listener.RecordListener

final class ReadHeader[T](cluster: AsyncCluster, policy: Policy, listener: Option[RecordListener[T]], key: Key)(implicit decoder: Decoder[T]) extends AsyncSingleCommand(cluster) {

  var record: Option[Record[T]] = None

  val partition = new Partition(key)

  def getPolicy: Policy = policy

  def writeBuffer(): Unit = setReadHeader(policy, key)

  def getNode: AsyncNode =
    cluster.getReadNode(partition, policy.replica).asInstanceOf[AsyncNode]

  def parseResult(byteBuffer: ByteBuffer): Unit = {
    val resultCode: Int = byteBuffer.get(5) & 0xFF
    if (resultCode == 0) {
      val generation: Int = byteBuffer.getInt(6)
      val expiration: Int = byteBuffer.getInt(10)
      record = Some(Record[T](None, generation, expiration))
    } else {
      if (resultCode != ResultCode.KEY_NOT_FOUND_ERROR) {
        throw new AerospikeException(resultCode)
      }
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
