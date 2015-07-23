package org.aerospiker

import com.aerospike.client.AerospikeClient

import policy._

object Client {
  def apply(p: ClientPolicy, hosts: Seq[Host]): Client = new Client(p, hosts)
}

class Client(p: ClientPolicy, hosts: Seq[Host])
  extends BaseClient(p, hosts)
  with Operation

class BaseClient(p: ClientPolicy, hosts: Seq[Host]) {

  val policy: ClientPolicy = p
  val asClient: AerospikeClient = new AerospikeClient(policy, hosts: _*)
  val asClientW: AerospikeClientWrapper = new AerospikeClientWrapper(asClient)

  def close(): Unit = {
    asClient.close()
  }

}
