package aerospiker
package command

import java.nio.ByteBuffer

import com.aerospike.client.{ AerospikeException, ResultCode }
import com.aerospike.client.async.{ AsyncCluster, AsyncCommand, AsyncSingleCommand }
import com.aerospike.client.cluster.{ Node, Partition }
import policy.WritePolicy
import listener.DeleteListener

final class Delete(
    cluster: AsyncCluster,
    policy: WritePolicy,
    listener: Option[DeleteListener],
    key: Key
) extends AsyncSingleCommand(cluster, policy) {

  private[this] val partition: Partition = new Partition(key)
  private[this] var existed: Boolean = false

  override def writeBuffer(): Unit = setDelete(policy, key)

  override def getNode: Node = cluster.getMasterNode(partition)

  override def parseResult(byteBuffer: ByteBuffer): Unit = {
    val resultCode: Int = byteBuffer.get(5) & 0xFF
    existed =
      if (resultCode == 0) true
      else if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) false
      else throw new AerospikeException(resultCode)
  }

  override def onSuccess(): Unit = listener.foreach(_.onSuccess(key, existed))

  override def onFailure(e: AerospikeException): Unit = listener.foreach(_.onFailure(e))

  override def cloneCommand(): AsyncCommand = new Delete(cluster, policy, listener, key)
}

