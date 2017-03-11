package aerospiker
package command

import java.nio.ByteBuffer

import aerospiker.listener.WriteListener
import aerospiker.policy.WritePolicy
import com.aerospike.client.AerospikeException
import com.aerospike.client.async.{ AsyncCluster, AsyncCommand, AsyncSingleCommand }
import com.aerospike.client.cluster.{ Node, Partition }

final class Touch(
    cluster: AsyncCluster,
    policy: WritePolicy,
    listener: Option[WriteListener],
    key: Key
) extends AsyncSingleCommand(cluster, policy) {

  private[this] val partition: Partition = new Partition(key)

  override def writeBuffer(): Unit = setTouch(policy, key)

  override def getNode: Node = cluster.getMasterNode(partition)

  override def parseResult(byteBuffer: ByteBuffer): Unit = {
    val resultCode: Int = byteBuffer.get(5) & 0xFF
    if (resultCode != 0) throw new AerospikeException(resultCode)
  }

  override def onSuccess(): Unit = listener.foreach(_.onSuccess(key))

  override def onFailure(e: AerospikeException): Unit = listener.foreach(_.onFailure(e))

  override def cloneCommand(): AsyncCommand = new Touch(cluster, policy, listener, key)
}
