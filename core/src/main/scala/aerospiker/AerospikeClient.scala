package aerospiker

import aerospiker.policy._
import com.aerospike.client.async.AsyncCluster

trait AerospikeClient {
  def policy: ClientPolicy
  def cluster: AsyncCluster
  def close(): Unit
}

object AerospikeClient {

  def apply(policy: ClientPolicy, hosts: Host*): AerospikeClient =
    new Impl(policy, hosts: _*)

  final class Impl(val policy: ClientPolicy, hosts: Host*) extends AerospikeClient {
    implicit val cluster = new AsyncCluster(policy, hosts.toArray)
    cluster.initTendThread(policy.failIfNotConnected)
    def close(): Unit = cluster.close()
  }

}
