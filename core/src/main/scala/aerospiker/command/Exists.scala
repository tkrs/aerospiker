package aerospiker
package command

import java.nio.ByteBuffer

import aerospiker.policy.Policy
import com.aerospike.client.{ AerospikeException, ResultCode }
import com.aerospike.client.async.{ AsyncCluster, AsyncCommand, AsyncSingleCommand }
import com.aerospike.client.cluster.{ Node, Partition }
import listener.ExistsListener

final class Exists(
    cluster: AsyncCluster,
    policy: Policy,
    listener: Option[ExistsListener],
    key: Key
) extends AsyncSingleCommand(cluster, policy) {

  private[this] val partition: Partition = new Partition(key)
  private[this] var exists = false

  override def writeBuffer(): Unit = setExists(policy, key)

  override def getNode: Node =
    cluster.getReadNode(partition, policy.replica)

  override def parseResult(byteBuffer: ByteBuffer): Unit = {
    val resultCode: Int = byteBuffer.get(5) & 0xFF
    if (resultCode == 0) exists = true
    else if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) exists = false
    else throw new AerospikeException(resultCode)
  }

  override def onSuccess(): Unit = listener.foreach(_.onSuccess(key, exists))

  override def onFailure(e: AerospikeException): Unit = listener.foreach(_.onFailure(e))

  override def cloneCommand(): AsyncCommand = new Exists(cluster, policy, listener, key)
}
