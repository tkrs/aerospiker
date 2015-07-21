package org.aerospiker

import com.aerospike.client.{ AerospikeClient }
import com.aerospike.client.policy.{ ClientPolicy => AsClientPolicy }

import Conversions._
import policy._

object Client {
  def apply(p: ClientPolicy, host: Seq[Host] = Seq(Host())): Client = new Client(p, host)
}

class Client(p: ClientPolicy, host: Seq[Host])
  extends BaseClient(p, host)
  with Operation

class BaseClient(p: ClientPolicy, host: Seq[Host])(
    implicit readPolicy: ReadPolicy = ReadPolicy(),
    writePolicy: WritePolicy = WritePolicy(),
    scanPolicy: ScanPolicy = ScanPolicy(),
    queryPolicy: QueryPolicy = QueryPolicy(),
    batchPolicy: BatchPolicy = BatchPolicy(),
    infoPolicy: InfoPolicy = InfoPolicy()) {

  val policy: AsClientPolicy = p
  val asClient: AerospikeClient = new AerospikeClient(policy, host.toAsHosts: _*)

  def close(): Unit = {
    asClient.close()
  }

}
