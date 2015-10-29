package aerospiker
package command

import java.nio.ByteBuffer

import aerospiker.listener.WriteListener
import com.aerospike.client.AerospikeException
import com.aerospike.client.async.{ AsyncCluster, AsyncNode, AsyncSingleCommand }
import com.aerospike.client.cluster.Partition
import com.aerospike.client.policy.{ Policy, WritePolicy }

final class Touch(cluster: AsyncCluster, policy: WritePolicy, listener: Option[WriteListener], key: Key) extends AsyncSingleCommand(cluster) {

  private val partition: Partition = new Partition(key)

  def getPolicy: Policy = policy

  def writeBuffer(): Unit = setTouch(policy, key)

  def getNode: AsyncNode = cluster.getMasterNode(partition).asInstanceOf[AsyncNode]

  def parseResult(byteBuffer: ByteBuffer): Unit = {
    val resultCode: Int = byteBuffer.get(5) & 0xFF
    if (resultCode != 0) throw new AerospikeException(resultCode)
  }

  def onSuccess(): Unit =
    listener match {
      case Some(l) => l.onSuccess(key)
      case None => // nop
    }

  def onFailure(e: AerospikeException): Unit =
    listener match {
      case Some(l) => l.onFailure(e)
      case None => // nop
    }
}
