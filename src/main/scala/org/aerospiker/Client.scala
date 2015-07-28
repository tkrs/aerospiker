package org.aerospiker

import com.aerospike.client.AerospikeClient

import policy._

object Client {
  def apply(hosts: Seq[Host])(implicit policy: DefaultPolicy[ClientPolicy]): Client = new Client(hosts)
}

class Client(hosts: Seq[Host])(implicit policy: DefaultPolicy[ClientPolicy]) extends Operation {

  val asClient: AerospikeClient = new AerospikeClient(policy(), hosts: _*)
  val asClientW: AerospikeClientWrapper = new AerospikeClientWrapper(asClient)

  def close() = asClient.close()

}
