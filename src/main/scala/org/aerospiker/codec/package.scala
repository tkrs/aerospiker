package org.aerospiker

import org.aerospiker.{ Client => AClient }
import org.aerospiker.policy.ClientPolicy

package object codec {

  type BinName = String
  type LDataKey = String
  type LDValue = Map[String, Any]

  object ClientFactory {
    def apply(host: String, port: Int)(implicit policy: ClientPolicy): AClient = {
      AClient(Seq(Host(host, port)))
    }
  }
}
