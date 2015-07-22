package org.aerospiker

import com.aerospike.client.AerospikeClient
import com.aerospike.client.policy.{ ClientPolicy => AsClientPolicy }

import Conversions._
import policy._

object Client {
  def apply(p: ClientPolicy, hosts: Seq[Host]): Client = new Client(p, hosts)
}

class Client(p: ClientPolicy, hosts: Seq[Host])
  extends BaseClient(p, hosts)
  with Operation

class BaseClient(p: ClientPolicy, hosts: Seq[Host]) {

  val policy: AsClientPolicy = p
  val asClient: AerospikeClient = new AerospikeClient(policy, hosts: _*)

  def close(): Unit = {
    asClient.close()
  }

}
