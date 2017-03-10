package aerospiker
package command

import aerospiker.policy.WritePolicy
import com.aerospike.client.Operation
import com.aerospike.client.async.AsyncCluster
import com.aerospike.client.cluster.Node
import io.circe.Decoder
import listener.RecordListener

final class Operate[A: Decoder](
    cluster: AsyncCluster,
    policy: WritePolicy,
    listener: Option[RecordListener[A]],
    key: Key, operations: Array[Operation]
) extends Read[A](cluster, policy, listener, key) {
  override def writeBuffer(): Unit = setOperate(policy, key, operations)
  override def getNode: Node = cluster.getMasterNode(partition)
}

