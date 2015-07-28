package org.aerospiker

import com.aerospike.client.AerospikeClient

import policy._

object Client {
  def apply(hosts: Seq[Host])(implicit policy: ClientPolicy): Client = new Client(hosts)(policy)
}

class Client(hosts: Seq[Host])(policy: ClientPolicy) extends Operation {

  val asClient: AerospikeClient = new AerospikeClient(policy, hosts: _*)
  val asClientW: AerospikeClientWrapper = new AerospikeClientWrapper(asClient)

  def close() = asClient.close()

}
