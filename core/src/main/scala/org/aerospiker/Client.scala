package org.aerospiker

import com.aerospike.client.async.AsyncClient

import policy.ClientPolicy

object Client {
  def apply(hosts: Seq[Host])(implicit policy: ClientPolicy): Client = new Client(hosts)(policy)
}

class Client(hosts: Seq[Host])(policy: ClientPolicy) extends Operation {

  val asClient = new AsyncClient(policy, hosts: _*)

  def close() = asClient.close()

}
