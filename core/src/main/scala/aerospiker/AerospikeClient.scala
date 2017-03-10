package aerospiker

import aerospiker.policy._
import com.aerospike.client.async.AsyncCluster

final case class AerospikeClient(policy: ClientPolicy, hosts: Host*) {
  implicit val cluster = new AsyncCluster(policy, hosts.toArray)
  cluster.initTendThread(policy.failIfNotConnected)
  def close(): Unit = cluster.close()
}
