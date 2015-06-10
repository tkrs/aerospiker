package org.aerospiker

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

// import com.aerospike.client.async.{ AsyncClient => AerospikeClient, AsyncClientPolicy => ClientPolicy }
import com.aerospike.client.{ Bin => AsBin, Key => AsKey, Record => AsRecord, AerospikeClient, AerospikeException }
import com.aerospike.client.policy.ClientPolicy
import scalaz._

object Client {
  def apply(settings: Settings): Client = {
    new Client(settings)
  }
}

class Client(settings: Settings)
  extends BaseClient(settings)
  with Operation

class BaseClient(settings: Settings) {

  final val asClient: AerospikeClient = {

    val policy = new ClientPolicy();
    policy.user = settings.user
    policy.password = settings.pwd
    policy.readPolicyDefault.maxRetries = settings.maxRetries
    policy.writePolicyDefault.maxRetries = settings.maxRetries
    // policy.asyncMaxCommands = 300;
    // policy.asyncSelectorThreads = 1;
    // policy.asyncSelectorTimeout = 10;
    policy.failIfNotConnected = true;

    val xs = settings.host.split(":")

    new AerospikeClient(policy, xs(0), xs(1).toInt);
  }

  def close(): Unit = {
    asClient.close()
  }

}
