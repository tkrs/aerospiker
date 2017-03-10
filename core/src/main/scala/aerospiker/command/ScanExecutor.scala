package aerospiker
package command

import aerospiker.listener.RecordSequenceListener
import com.aerospike.client.AerospikeException
import com.aerospike.client.ResultCode
import com.aerospike.client.async.{ AsyncCluster, AsyncNode, AsyncMultiExecutor }
import com.aerospike.client.cluster.Node

import aerospiker.policy.ScanPolicy
import io.circe.Decoder

final class ScanExecutor[A: Decoder](
    cluster: AsyncCluster,
    policy: ScanPolicy,
    listener: Option[RecordSequenceListener[A]],
    namespace: String,
    setName: String,
    binNames: Array[String]
) extends AsyncMultiExecutor {
  private[this] val nodes: Array[Node] = cluster.getNodes

  if (nodes.length == 0) {
    throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Scan failed because cluster is empty.")
  }
  private[this] val taskId: Long = System.nanoTime

  def execute(): Unit = {
    val tasks: Array[Scan[A]] =
      nodes.map(node =>
        new Scan[A](this, cluster, node.asInstanceOf[AsyncNode], policy, listener, namespace, setName, binNames, taskId))
    execute(tasks.toArray, policy.maxConcurrentNodes)
  }

  override def onSuccess(): Unit = listener match {
    case Some(l) => l.onSuccess()
    case None => // nop
  }

  override def onFailure(ae: AerospikeException): Unit = listener match {
    case Some(l) => l.onFailure(ae)
    case None => // nop
  }
}
