package aerospiker
package command

import java.nio.ByteBuffer

import com.aerospike.client.{ AerospikeException, ResultCode }
import com.aerospike.client.async.{ AsyncCluster, AsyncCommand, AsyncSingleCommand }
import com.aerospike.client.cluster.{ Node, Partition }
import com.aerospike.client.policy.Policy
import io.circe.Decoder
import listener.RecordListener

final class ReadHeader[T: Decoder](cluster: AsyncCluster, policy: Policy, listener: Option[RecordListener[T]], key: Key) extends AsyncSingleCommand(cluster, policy) {

  var record: Option[Record[T]] = None

  val partition = new Partition(key)

  override def writeBuffer(): Unit = setReadHeader(policy, key)

  override def getNode: Node =
    cluster.getReadNode(partition, policy.replica)

  override def parseResult(byteBuffer: ByteBuffer): Unit = {
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

  override def onSuccess(): Unit = listener.foreach(_.onSuccess(key, record))

  override def onFailure(e: AerospikeException): Unit = listener.foreach(_.onFailure(e))

  override def cloneCommand(): AsyncCommand = new ReadHeader[T](cluster, policy, listener, key)
}
