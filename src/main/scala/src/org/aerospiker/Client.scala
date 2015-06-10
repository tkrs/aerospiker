package org.aerospiker

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

// import com.aerospike.client.async.{ AsyncClient => AerospikeClient, AsyncClientPolicy => ClientPolicy }
import com.aerospike.client.{ Bin => AsBin, Key => AsKey, Record => AsRecord, AerospikeClient, AerospikeException }
import com.aerospike.client.policy.ClientPolicy
import scalaz._

object Client {
  def apply(host: String, user: String, pwd: String): Client = {
    new Client(host, user, pwd)
  }
}

class Client(host: String, user: String, pwd: String)
  extends BaseClient(host, user, pwd)
  with Operation

class BaseClient(host: String, user: String, pwd: String) {

  final val asClient: AerospikeClient = {

    val policy = new ClientPolicy();
    policy.user = user
    policy.password = pwd
    // policy.asyncMaxCommands = 300;
    // policy.asyncSelectorThreads = 1;
    // policy.asyncSelectorTimeout = 10;
    policy.failIfNotConnected = true;

    val xs = host.split(":")

    new AerospikeClient(policy, xs(0), xs(1).toInt);
  }

  def close(): Unit = {
    asClient.close()
  }

}
