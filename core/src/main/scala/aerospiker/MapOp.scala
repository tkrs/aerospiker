package aerospiker

import com.aerospike.client.Operation

sealed trait MapOp[A]

object MapOp {
  import cdt._
  final case class SetMapPolicy(binName: String, attribute: MapOrder) extends MapOp[Operation]
  final case class CreatePut[A, B](policy: MapPolicy, binName: String, a: A, b: B) extends MapOp[Operation]
  final case class CreateOperation[A, B](policy: MapPolicy, binName: String, a: A, b: B) extends MapOp[Operation]
}
