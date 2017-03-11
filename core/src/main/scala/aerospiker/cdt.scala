package aerospiker

/**
 * Created by satotakeru on 2017/03/11.
 */
object cdt {
  sealed abstract class Op()

  object mapCommand {
    private[cdt] val SetType = 64
    private[cdt] val Add = 65
    private[cdt] val AddItems = 66
    private[cdt] val Put = 67
    private[cdt] val PutItems = 68
    private[cdt] val Replace = 69
    private[cdt] val ReplaceItems = 70
    private[cdt] val INCREMENT = 73
    private[cdt] val DECREMENT = 74
    private[cdt] val CLEAR = 75
    private[cdt] val REMOVE_BY_KEY = 76
    private[cdt] val REMOVE_BY_INDEX = 77
    private[cdt] val REMOVE_BY_RANK = 79
    private[cdt] val REMOVE_BY_KEY_LIST = 81
    private[cdt] val REMOVE_BY_VALUE = 82
    private[cdt] val REMOVE_BY_VALUE_LIST = 83
    private[cdt] val REMOVE_BY_KEY_INTERVAL = 84
    private[cdt] val REMOVE_BY_INDEX_RANGE = 85
    private[cdt] val REMOVE_BY_VALUE_INTERVAL = 86
    private[cdt] val REMOVE_BY_RANK_RANGE = 87
    private[cdt] val SIZE = 96
    private[cdt] val GET_BY_KEY = 97
    private[cdt] val GET_BY_INDEX = 98
    private[cdt] val GET_BY_RANK = 100
    private[cdt] val GET_BY_VALUE = 102
    private[cdt] val GET_BY_KEY_INTERVAL = 103
    private[cdt] val GET_BY_INDEX_RANGE = 104
    private[cdt] val GET_BY_VALUE_INTERVAL = 105
    private[cdt] val GET_BY_RANK_RANGE = 106
  }

  sealed abstract class MapReturnType(value: Int)
  object MapReturnType {
    final case object None extends MapReturnType(0)
    final case object Index extends MapReturnType(1)
    final case object ReverseIndex extends MapReturnType(2)
    final case object Rank extends MapReturnType(3)
    final case object ReverseRank extends MapReturnType(4)
    final case object Count extends MapReturnType(5)
    final case object Key extends MapReturnType(6)
    final case object Value extends MapReturnType(7)
    final case object KeyValue extends MapReturnType(8)
  }

  sealed abstract class MapOrder(value: Int)
  object MapOrder {
    final case object Unordered extends MapOrder(0)
    final case object KeyOrdered extends MapOrder(1)
    final case object KeyValueOrdered extends MapOrder(3)
  }

  sealed abstract class MapWriteMode(itemCommand: Int, itemsCommand: Int)
  object MapWriteMode {
    import mapCommand._
    final case object Update extends MapWriteMode(Put, PutItems)
    final case object UpdateOnly extends MapWriteMode(Replace, ReplaceItems)
    final case object CreateOnly extends MapWriteMode(Add, AddItems)
  }

  final case class MapPolicy(
    oder: MapOrder,
    writeMode: MapWriteMode
  )

}
